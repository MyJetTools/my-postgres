#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
use rust_extensions::{date_time::DateTimeAsMicroseconds, StrOrString};
#[cfg(feature = "with-logs-and-telemetry")]
use std::collections::HashMap;
use tokio::{sync::RwLock, time::error::Elapsed};
use tokio_postgres::{NoTls, Row};

#[cfg(feature = "with-tls")]
use openssl::ssl::{SslConnector, SslMethod};
#[cfg(feature = "with-tls")]
use postgres_openssl::MakeTlsConnector;

#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{
    sql::{SqlData, SqlValues},
    MyPostgresError, PostgresSettings,
};

pub struct PostgresConnectionInstance {
    client: Arc<RwLock<Option<tokio_postgres::Client>>>,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Send + Sync + 'static>,
    pub connected: Arc<AtomicBool>,
    pub created: DateTimeAsMicroseconds,
    pub sql_request_timeout: Duration,
}

impl PostgresConnectionInstance {
    pub fn new(
        app_name: StrOrString<'static>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        let client = Arc::new(RwLock::new(None));
        let connected = Arc::new(AtomicBool::new(false));

        #[cfg(feature = "with-logs-and-telemetry")]
        let logger_spawned = logger.clone();

        tokio::spawn(establish_connection_loop(
            app_name,
            postgres_settings,
            client.clone(),
            connected.clone(),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger_spawned,
        ));

        Self {
            client,
            connected,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
            created: DateTimeAsMicroseconds::now(),
            sql_request_timeout,
        }
    }

    pub fn disconnect(&self) {
        self.connected
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn execute_sql(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let connection_access = self.client.read().await;

        if connection_access.is_none() {
            return Err(MyPostgresError::NoConnection);
        }

        let connection_access = connection_access.as_ref().unwrap();

        let params = sql.values.get_values_to_invoke();

        let execution = connection_access.execute(&sql.sql, params.as_slice());

        if std::env::var("DEBUG").is_ok() {
            println!("SQL: {}", &sql.sql);
        }

        let process_name = if let Some(process_name) = process_name {
            process_name
        } else {
            sql.get_sql_as_process_name()
        };

        #[cfg(feature = "with-logs-and-telemetry")]
        let started = DateTimeAsMicroseconds::now();

        let result = execute_with_timeout(
            process_name,
            Some(&sql.sql),
            execution,
            self.sql_request_timeout,
            &self.connected,
            #[cfg(feature = "with-logs-and-telemetry")]
            &self.logger,
            #[cfg(feature = "with-logs-and-telemetry")]
            started,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await;

        Ok(self.handle_error(result)?)
    }

    pub async fn execute_bulk_sql<'s>(
        &self,
        sql_with_params: Vec<(String, SqlValues)>,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        if std::env::var("DEBUG").is_ok() {
            if let Some(first_value) = sql_with_params.get(0) {
                println!("SQL: {:?}", first_value.0);
            }
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        let started = DateTimeAsMicroseconds::now();

        let mut connection_access = self.client.write().await;

        if connection_access.is_none() {
            return Err(MyPostgresError::NoConnection);
        }

        let connection_access = connection_access.as_mut().unwrap();

        let execution = {
            let builder = connection_access.build_transaction();
            let transaction = builder.start().await?;

            for (sql, params) in &sql_with_params {
                transaction
                    .execute(sql, &params.get_values_to_invoke())
                    .await?;
            }
            transaction.commit()
        };

        let result = execute_with_timeout(
            process_name,
            None,
            execution,
            self.sql_request_timeout,
            &self.connected,
            #[cfg(feature = "with-logs-and-telemetry")]
            &self.logger,
            #[cfg(feature = "with-logs-and-telemetry")]
            started,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await;

        self.handle_error(result)
    }

    pub async fn execute_sql_as_vec<'s, TEntity, TTransform: Fn(&Row) -> TEntity>(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let connection_access = self.client.read().await;

        if connection_access.is_none() {
            return Err(MyPostgresError::NoConnection);
        }

        let connection_access = connection_access.as_ref().unwrap();

        if std::env::var("DEBUG").is_ok() {
            println!("SQL: {}", &sql.sql);
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        let started = DateTimeAsMicroseconds::now();

        let params = sql.values.get_values_to_invoke();
        let execution = connection_access.query(&sql.sql, params.as_slice());

        let process_name = if let Some(process_name) = process_name {
            process_name
        } else {
            sql.get_sql_as_process_name()
        };

        let result = execute_with_timeout(
            process_name,
            Some(&sql.sql),
            execution,
            self.sql_request_timeout,
            &self.connected,
            #[cfg(feature = "with-logs-and-telemetry")]
            &self.logger,
            #[cfg(feature = "with-logs-and-telemetry")]
            started,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await;

        let result = self.handle_error(result)?.iter().map(transform).collect();

        Ok(result)
    }

    fn handle_error<TResult>(
        &self,
        result: Result<TResult, MyPostgresError>,
    ) -> Result<TResult, MyPostgresError> {
        if let Err(err) = &result {
            match err {
                MyPostgresError::NoConnection => {}
                MyPostgresError::SingleRowRequestReturnedMultipleRows(_) => {}
                MyPostgresError::PostgresError(_) => {}
                MyPostgresError::Other(_) => {
                    self.disconnect();
                }
                MyPostgresError::TimeOut(_) => {
                    self.disconnect();
                }
            }
        }

        result
    }
}

async fn execute_with_timeout<
    TResult,
    TFuture: Future<Output = Result<TResult, tokio_postgres::Error>>,
>(
    process_name: &str,
    sql: Option<&str>,
    execution: TFuture,
    sql_request_timeout: Duration,
    connected: &Arc<AtomicBool>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Send + Sync + 'static>,
    #[cfg(feature = "with-logs-and-telemetry")] started: DateTimeAsMicroseconds,
    #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
) -> Result<TResult, MyPostgresError> {
    let timeout_result: Result<Result<TResult, tokio_postgres::Error>, Elapsed> =
        tokio::time::timeout(sql_request_timeout, execution).await;

    let result = if timeout_result.is_err() {
        connected.store(false, Ordering::SeqCst);
        Err(MyPostgresError::TimeOut(sql_request_timeout))
    } else {
        match timeout_result.unwrap() {
            Ok(result) => Ok(result),
            Err(err) => Err(MyPostgresError::PostgresError(err)),
        }
    };

    if let Err(err) = &result {
        println!(
            "{}: Execution request {} finished with error {:?}",
            DateTimeAsMicroseconds::now().to_rfc3339(),
            process_name,
            err
        );

        if let Some(sql) = sql {
            let sql = if sql.len() > 255 { &sql[..255] } else { sql };
            println!("SQL: {}", sql);
        }
    }

    #[cfg(feature = "with-logs-and-telemetry")]
    if let Some(telemetry_context) = &telemetry_context {
        match &result {
            Ok(_) => {
                my_telemetry::TELEMETRY_INTERFACE
                    .write_success(
                        telemetry_context,
                        started,
                        process_name.to_string(),
                        "Ok".to_string(),
                        None,
                    )
                    .await;
            }
            Err(err) => {
                write_fail_telemetry_and_log(
                    started,
                    "execute_sql".to_string(),
                    Some(process_name),
                    format!("{:?}", err),
                    telemetry_context,
                    logger,
                )
                .await;
            }
        }
    }

    result
}

#[cfg(feature = "with-logs-and-telemetry")]
async fn write_fail_telemetry_and_log(
    started: DateTimeAsMicroseconds,
    process: String,
    sql: Option<&str>,
    fail: String,
    telemetry_context: &MyTelemetryContext,
    logger: &Arc<dyn Logger + Send + Sync + 'static>,
) {
    let ctx = if let Some(sql) = sql {
        let mut ctx = HashMap::new();
        ctx.insert("sql".to_string(), sql.to_string());
        Some(ctx)
    } else {
        None
    };

    logger.write_error(process.to_string(), fail.to_string(), ctx);

    if !my_telemetry::TELEMETRY_INTERFACE.is_telemetry_set_up() {
        return;
    }
    my_telemetry::TELEMETRY_INTERFACE
        .write_fail(telemetry_context, started, process, fail, None)
        .await;
}

async fn establish_connection_loop(
    app_name: StrOrString<'static>,
    postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    client: Arc<RwLock<Option<tokio_postgres::Client>>>,
    connected: Arc<AtomicBool>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
) {
    loop {
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
            create_and_start_no_tls(
                conn_string,
                &client,
                &connected,
                #[cfg(feature = "with-logs-and-telemetry")]
                &logger,
            )
            .await;
        }

        let mut write_access = client.write().await;
        write_access.take();
    }
}

async fn create_and_start_no_tls(
    connection_string: String,
    client: &Arc<RwLock<Option<tokio_postgres::Client>>>,
    connected: &Arc<AtomicBool>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let result = tokio_postgres::connect(connection_string.as_str(), NoTls).await;

    match result {
        Ok((postgres_client, postgres_connection)) => {
            let connected_date_time =
                handle_connection_is_established(client, postgres_client, connected).await;

            let connected_spawned = connected.clone();

            #[cfg(feature = "with-logs-and-telemetry")]
            let logger_spawned = logger.clone();

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

                connected_spawned.store(false, Ordering::SeqCst);
            });

            while connected.load(Ordering::Relaxed) {
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

async fn handle_connection_is_established(
    client: &Arc<RwLock<Option<tokio_postgres::Client>>>,
    postgres_client: tokio_postgres::Client,
    connected: &Arc<AtomicBool>,
) -> DateTimeAsMicroseconds {
    let connected_date_time = DateTimeAsMicroseconds::now();
    println!(
        "{}: Postgres SQL Connection is established",
        connected_date_time.to_rfc3339()
    );

    {
        let mut write_access = client.write().await;
        *write_access = Some(postgres_client);
    };
    connected.store(true, Ordering::SeqCst);

    connected_date_time
}
