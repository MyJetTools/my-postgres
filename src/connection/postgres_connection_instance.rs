#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
use rust_extensions::{date_time::DateTimeAsMicroseconds, StrOrString};

use tokio_postgres::{NoTls, Row};

#[cfg(feature = "with-tls")]
use openssl::ssl::{SslConnector, SslMethod};
#[cfg(feature = "with-tls")]
use postgres_openssl::MakeTlsConnector;

#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::{sync::Arc, time::Duration};

use crate::{
    count_result::CountResult, sql::SqlData, ConnectionString, MyPostgresError, PostgresSettings,
};

use super::PostgresConnectionInner;

pub struct PostgresConnectionInstance {
    inner: Arc<PostgresConnectionInner>,
    #[cfg(feature = "with-logs-and-telemetry")]
    pub logger: Arc<dyn Logger + Send + Sync + 'static>,
    pub created: DateTimeAsMicroseconds,
    pub postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    pub app_name: StrOrString<'static>,
}

impl PostgresConnectionInstance {
    pub fn new(
        app_name: StrOrString<'static>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        let inner = Arc::new(PostgresConnectionInner::new(
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger.clone(),
        ));

        #[cfg(feature = "with-logs-and-telemetry")]
        let logger_spawned = logger.clone();

        tokio::spawn(establish_connection_loop(
            app_name.as_str().to_string(),
            postgres_settings.clone(),
            inner.clone(),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger_spawned,
        ));

        Self {
            inner,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
            created: DateTimeAsMicroseconds::now(),
            postgres_settings,
            app_name,
        }
    }

    pub fn disconnect(&self) {
        self.inner.disconnect();
    }

    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    pub async fn get_db_name(&self) -> String {
        let conn_string = self.postgres_settings.get_connection_string().await;

        let conn_string_format =
            crate::ConnectionStringFormat::parse_and_detect(conn_string.as_str());

        let connection_string = ConnectionString::parse(conn_string_format);

        connection_string.get_db_name().to_string()
    }

    pub async fn execute_sql(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.inner
            .execute_sql(
                sql,
                process_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_bulk_sql<'s>(
        &self,
        sql_with_params: Vec<SqlData>,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.inner
            .execute_bulk_sql(
                sql_with_params,
                process_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_sql_as_vec<'s, TEntity, TTransform: Fn(&Row) -> TEntity>(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let result_rows_set = self
            .inner
            .execute_sql_as_vec(
                sql,
                process_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        let mut result = Vec::with_capacity(result_rows_set.len());

        for row in result_rows_set {
            result.push(transform(&row));
        }

        Ok(result)
    }

    pub async fn get_count<TCountResult: CountResult>(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TCountResult>, MyPostgresError> {
        let mut result = self
            .execute_sql_as_vec(
                sql,
                process_name,
                |db_row| TCountResult::from_db_row(db_row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        if result.len() > 0 {
            Ok(Some(result.remove(0)))
        } else {
            Ok(None)
        }
    }
}

async fn establish_connection_loop(
    app_name: String,
    postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    inner: Arc<PostgresConnectionInner>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
) {
    loop {
        if inner.is_to_be_disposable() {
            break;
        }

        let conn_string = postgres_settings.get_connection_string().await;

        let conn_string = super::connection_string::format(conn_string.as_str(), app_name.as_str());

        if conn_string.contains("sslmode=require") {
            #[cfg(feature = "with-tls")]
            create_and_start_with_tls(
                conn_string,
                &client,
                &connected,
                #[cfg(feature = "with-logs-and-telemetry")]
                &logger,
            )
            .await;
            #[cfg(not(feature = "with-tls"))]
            {
                #[cfg(feature = "with-logs-and-telemetry")]
                logger.write_error(
                    "PostgresConnection".to_string(),
                    "Postgres connection with sslmode=require is not supported without tls feature"
                        .to_string(),
                    None,
                );

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        } else {
            create_and_start_no_tls_connection(
                conn_string,
                &inner,
                #[cfg(feature = "with-logs-and-telemetry")]
                &logger,
            )
            .await;
        }

        inner.disconnect();
    }

    println!("Postgres Connection loop is stopped");
}

async fn create_and_start_no_tls_connection(
    connection_string: String,
    inner: &Arc<PostgresConnectionInner>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let result = tokio_postgres::connect(connection_string.as_str(), NoTls).await;

    match result {
        Ok((postgres_client, postgres_connection)) => {
            let connected_date_time = inner
                .handle_connection_is_established(postgres_client)
                .await;

            #[cfg(feature = "with-logs-and-telemetry")]
            let logger_spawned = logger.clone();

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
                        logger_spawned.write_fatal_error(
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
                "{}: Can not establish postgres connection with Err: {:?}",
                DateTimeAsMicroseconds::now().to_rfc3339(),
                err
            );

            #[cfg(feature = "with-logs-and-telemetry")]
            logger.write_fatal_error(
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
    client: &Arc<RwLock<Option<tokio_postgres::Client>>>,
    connected: &Arc<AtomicBool>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let builder = SslConnector::builder(SslMethod::tls()).unwrap();

    let connector = MakeTlsConnector::new(builder.build());

    let result = tokio_postgres::connect(connection_string.as_str(), connector).await;
    #[cfg(feature = "with-logs-and-telemetry")]
    let logger_spawned = logger.clone();
    match result {
        Ok((postgres_client, postgres_connection)) => {
            let connected_date_time =
                handle_connection_is_established(client, postgres_client, connected).await;

            let connected_spawned = connected.clone();

            tokio::spawn(async move {
                if let Err(e) = postgres_connection.await {
                    eprintln!(
                        "Connection started at {} has error: {}",
                        connected_date_time.to_rfc3339(),
                        e
                    );
                }
                #[cfg(feature = "with-logs-and-telemetry")]
                logger_spawned.write_fatal_error(
                    "Postgres background".to_string(),
                    format!("Exist connection loop"),
                    None,
                );

                connected_spawned.store(false, Ordering::SeqCst);
            });

            while connected.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(_err) => {
            #[cfg(feature = "with-logs-and-telemetry")]
            logger.write_fatal_error(
                "Creating Postgres".to_string(),
                format!("Invalid connection string. {:?}", _err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

impl Drop for PostgresConnectionInstance {
    fn drop(&mut self) {
        self.inner.set_to_be_disposable();
    }
}
