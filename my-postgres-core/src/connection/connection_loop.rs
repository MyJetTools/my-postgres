use std::{sync::Arc, time::Duration};

use my_logger::LogEventCtx;
use tokio_postgres::NoTls;

#[cfg(feature = "with-tls")]
use openssl::ssl::{SslConnector, SslMethod};
#[cfg(feature = "with-tls")]
use postgres_openssl::MakeTlsConnector;

use crate::PostgresConnectionString;

use super::postgres_connect_inner::PostgresConnectionInner;

pub async fn start_connection_loop(
    inner: Arc<PostgresConnectionInner>,
    db_name: String,
    #[cfg(feature = "with-ssh")] ssh_config: Option<crate::ssh::PostgresSshConfig>,
) {
    loop {
        if inner.is_to_be_disposable() {
            break;
        }

        let conn_string = inner.postgres_settings.get_connection_string().await;

        #[cfg(feature = "with-ssh")]
        let mut conn_string = PostgresConnectionString::from_str(conn_string.as_str());

        #[cfg(not(feature = "with-ssh"))]
        let conn_string = PostgresConnectionString::from_str(conn_string.as_str());

        #[cfg(feature = "with-ssh")]
        let postgres_host = if let Some(ssh_config) = &ssh_config {
            let ssh_cred_type = match ssh_config.credentials.as_ref() {
                my_ssh::SshCredentials::SshAgent { .. } => "SshAgent",
                my_ssh::SshCredentials::UserNameAndPassword { .. } => "UserNameAndPassword",
                my_ssh::SshCredentials::PrivateKey { .. } => "PrivateKey",
            };

            let (ssh_host, ssh_port) = ssh_config.credentials.get_host_port();
            format!(
                "[{}]ssh:{}:{}->{}:{}",
                ssh_cred_type,
                ssh_host,
                ssh_port,
                conn_string.get_host(),
                conn_string.get_port()
            )
        } else {
            format!("{}:{}", conn_string.get_host(), conn_string.get_port())
        };

        #[cfg(not(feature = "with-ssh"))]
        let postgres_host = format!("{}:{}", conn_string.get_host(), conn_string.get_port());

        #[cfg(feature = "with-ssh")]
        if let Some(ssh_config) = &ssh_config {
            crate::ssh::start_ssh_tunnel_and_get_connection_string(&mut conn_string, ssh_config)
                .await;
        }

        if conn_string.get_ssl_require() {
            #[cfg(feature = "with-tls")]
            {
                println!("Starting TLS Postgres connection to {postgres_host} for db {db_name}.");
                create_and_start_with_tls(conn_string, &inner, postgres_host).await;
            }

            #[cfg(not(feature = "with-tls"))]
            {
                println!("Can not Start Postgres connection to {postgres_host} for db {db_name}. Please enabled tls feature.");

                my_logger::LOGGER.write_error(
                    "PostgresConnection",
                    "Postgres connection with sslmode=require is not supported without tls feature",
                    LogEventCtx::new()
                        .add("Host", postgres_host)
                        .add("DbName", db_name.to_string())
                        .add("TLS", "true"),
                );

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        } else {
            println!("Starting Postgres connection to {postgres_host} for db {db_name}");
            create_and_start_no_tls_connection(conn_string, &inner, postgres_host).await;
        }

        inner.disconnect();
    }

    if std::env::var("DEBUG_SQL").is_ok() {
        println!("Postgres Connection loop is stopped");
    }
}

async fn create_and_start_no_tls_connection(
    connection_string: PostgresConnectionString,
    inner: &Arc<PostgresConnectionInner>,
    postgres_host: String,
) {
    let mut ctx = LogEventCtx::new()
        .add("Host", postgres_host.to_string())
        .add("db_name", connection_string.get_db_name());

    let connection_string = connection_string.to_string(&inner.app_name);

    let result = tokio_postgres::connect(connection_string.as_str(), NoTls).await;

    match result {
        Ok((postgres_client, postgres_connection)) => {
            let connected_date_time = inner
                .handle_connection_is_established(postgres_client, &postgres_host, false)
                .await;

            ctx = ctx.add("Connected".to_string(), connected_date_time.to_rfc3339());

            let ctx_spawned = ctx.clone();
            let inner_spawned = inner.clone();
            tokio::spawn(async move {
                match postgres_connection.await {
                    Ok(_) => {
                        my_logger::LOGGER.write_debug(
                            "Postgres background".to_string(),
                            format!("Exist connection loop gracefully"),
                            ctx_spawned,
                        );
                    }
                    Err(err) => {
                        my_logger::LOGGER.write_debug(
                            "Postgres background".to_string(),
                            format!("Exist connection loop with error: {:?}", err),
                            ctx_spawned,
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
            my_logger::LOGGER.write_fatal_error(
                "Connecting to postgres".to_string(),
                format!("Can not establish postgres connection. {:?}", err),
                ctx,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(feature = "with-tls")]
async fn create_and_start_with_tls(
    connection_string: PostgresConnectionString,
    inner: &Arc<PostgresConnectionInner>,
    postgres_host: String,
) {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();

    builder.set_verify_callback(openssl::ssl::SslVerifyMode::all(), |_, _| true);

    let connector = MakeTlsConnector::new(builder.build());

    let ctx = LogEventCtx::new().add("host", postgres_host.clone());

    let connection_string = connection_string.to_string(&inner.app_name);

    let result = tokio_postgres::connect(connection_string.as_str(), connector).await;

    match result {
        Ok((postgres_client, postgres_connection)) => {
            let connected_date_time = inner
                .handle_connection_is_established(postgres_client, &postgres_host, true)
                .await;

            ctx.clone()
                .add("Connected".to_string(), connected_date_time.to_rfc3339());

            let ctx_spawned = ctx.clone();

            let inner_spawned = inner.clone();

            tokio::spawn(async move {
                if let Err(e) = postgres_connection.await {
                    my_logger::LOGGER.write_fatal_error(
                        "Connecting to Postgres via TLS".to_string(),
                        format!("Can not establish postgres connection. {:?}", e),
                        ctx_spawned,
                    );
                }

                inner_spawned.disconnect();
            });

            while inner.is_connected() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(err) => {
            my_logger::LOGGER.write_fatal_error(
                "Connecting to Postgres via TLS".to_string(),
                format!("Can not establish postgres connection. {:?}", err),
                ctx,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
