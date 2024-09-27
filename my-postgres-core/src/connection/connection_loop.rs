use std::{sync::Arc, time::Duration};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio_postgres::NoTls;

#[cfg(feature = "with-tls")]
use openssl::ssl::{SslConnector, SslMethod};
#[cfg(feature = "with-tls")]
use postgres_openssl::MakeTlsConnector;

use crate::PostgresConnectionString;

pub const POSTGRES_DEFAULT_PORT: u16 = 5432;

use super::postgres_connect_inner::PostgresConnectionInner;

pub async fn start_connection_loop(
    inner: Arc<PostgresConnectionInner>,
    db_name: String,
    #[cfg(feature = "with-ssh")] ssh_target: Arc<crate::ssh::SshTarget>,
) {
    loop {
        if inner.is_to_be_disposable() {
            break;
        }

        let conn_string = inner.postgres_settings.get_connection_string().await;

        let conn_string =
            super::connection_string::format(conn_string.as_str(), inner.app_name.as_str());

        let my_conn_string = get_conn_string(&conn_string);

        if my_conn_string.get_ssl_require() {
            #[cfg(feature = "with-tls")]
            {
                println!("Starting Postgres connection for db {db_name} with SSLMODE=require. 'with-tls' feature is enabled");
                create_and_start_with_tls(conn_string, &inner).await;
            }

            #[cfg(not(feature = "with-tls"))]
            {
                println!("Starting Postgres connection for db {db_name}  with SSLMODE=require. 'with-tls' feature is disabled");
                #[cfg(feature = "with-logs-and-telemetry")]
                inner.logger.write_error(
                    "PostgresConnection".to_string(),
                    "Postgres connection with sslmode=require is not supported without tls feature"
                        .to_string(),
                    None,
                );

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        } else {
            println!("Starting Postgres connection for db {db_name} with NO sslmode=require");
            create_and_start_no_tls_connection(
                conn_string,
                &inner,
                #[cfg(feature = "with-ssh")]
                &ssh_target,
            )
            .await;
        }

        inner.disconnect();
    }

    if std::env::var("DEBUG").is_ok() {
        println!("Postgres Connection loop is stopped");
    }
}

async fn create_and_start_no_tls_connection(
    connection_string: String,
    inner: &Arc<PostgresConnectionInner>,
    #[cfg(feature = "with-ssh")] ssh_target: &Arc<crate::ssh::SshTarget>,
) {
    #[cfg(feature = "with-ssh")]
    let (result, postgres_host) = if let Some(ssh_target) = ssh_target.get_value().await {
        let ssh_session = ssh_target.get_ssh_session().await;

        let connection_string = PostgresConnectionString::from_str(connection_string.as_str());

        let get_host_endpoint = connection_string.get_host_endpoint();

        let (host, port) = crate::ssh::generate_unix_socket_file(
            ssh_target.credentials.as_ref().unwrap(),
            get_host_endpoint,
        );

        println!(
            "Establishing Postgres SSH connection through {}:{}",
            host, port
        );
        let result = ssh_session
            .start_port_forward(
                format!("{}:{}", host, port),
                get_host_endpoint.host.to_string(),
                get_host_endpoint.port.unwrap_or(POSTGRES_DEFAULT_PORT),
            )
            .await;

        if let Err(result) = result {
            println!("Can not start port forwarding with error: {:?}", result);
        }

        let con_string = connection_string.to_string_new_host_port(&host, port, &inner.app_name);

        (
            tokio_postgres::connect(con_string.as_str(), NoTls).await,
            format!("{}:{}", host, port),
        )
    } else {
        #[cfg(feature = "with-ssh")]
        println!("Postgres SSH connection is not set up");

        let cs = PostgresConnectionString::from_str(connection_string.as_str());
        (
            tokio_postgres::connect(connection_string.as_str(), NoTls).await,
            format!("{:?}", cs.get_host_endpoint()),
        )
    };

    #[cfg(not(feature = "with-ssh"))]
    let result = tokio_postgres::connect(connection_string.as_str(), NoTls).await;

    match result {
        Ok((postgres_client, postgres_connection)) => {
            let connected_date_time = inner
                .handle_connection_is_established(postgres_client)
                .await;

            #[cfg(feature = "with-logs-and-telemetry")]
            let logger_spawned = inner.logger.clone();

            let inner_spawned = inner.clone();

            tokio::spawn(async move {
                match postgres_connection.await {
                    Ok(_) => {
                        println!(
                            "{}: Connection established at {} is closed.",
                            DateTimeAsMicroseconds::now().to_rfc3339(),
                            connected_date_time.to_rfc3339(),
                        );
                    }
                    Err(err) => {
                        println!(
                            "{}: Connection established at {} is closed with error: {}",
                            DateTimeAsMicroseconds::now().to_rfc3339(),
                            connected_date_time.to_rfc3339(),
                            err
                        );

                        #[cfg(feature = "with-logs-and-telemetry")]
                        logger_spawned.write_debug_info(
                            "Postgres background".to_string(),
                            format!("Exist connection loop"),
                            None,
                        );
                    }
                }

                inner_spawned.disconnect();
            });

            while inner.is_connected() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(err) => {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            println!(
                "{}: Can not establish postgres connection with {} Err: {:?}",
                DateTimeAsMicroseconds::now().to_rfc3339(),
                postgres_host,
                err
            );

            #[cfg(feature = "with-logs-and-telemetry")]
            inner.logger.write_fatal_error(
                "CreatingPostgres".to_string(),
                format!("Can not establish postgres connection. {:?}", err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(feature = "with-tls")]
async fn create_and_start_with_tls(
    connection_string: String,
    inner: &Arc<PostgresConnectionInner>,
) {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();

    builder.set_verify_callback(openssl::ssl::SslVerifyMode::all(), |_, _| true);

    let connector = MakeTlsConnector::new(builder.build());

    let result = tokio_postgres::connect(connection_string.as_str(), connector).await;
    #[cfg(feature = "with-logs-and-telemetry")]
    let logger_spawned = inner.logger.clone();
    match result {
        Ok((postgres_client, postgres_connection)) => {
            let connected_date_time = inner
                .handle_connection_is_established(postgres_client)
                .await;

            let inner_spawned = inner.clone();

            tokio::spawn(async move {
                if let Err(e) = postgres_connection.await {
                    eprintln!(
                        "Connection started at {} has error: {}",
                        connected_date_time.to_rfc3339(),
                        e
                    );
                }
                #[cfg(feature = "with-logs-and-telemetry")]
                logger_spawned.write_debug_info(
                    "Postgres background".to_string(),
                    format!("Exist connection loop"),
                    None,
                );

                inner_spawned.disconnect();
            });

            while inner.is_connected() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(_err) => {
            #[cfg(feature = "with-logs-and-telemetry")]
            inner.logger.write_fatal_error(
                "Creating Postgres".to_string(),
                format!("Invalid connection string. {:?}", _err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn get_conn_string(src: &str) -> PostgresConnectionString {
    let conn_string_format = crate::ConnectionStringFormat::parse_and_detect(src);
    PostgresConnectionString::parse(conn_string_format)
}
