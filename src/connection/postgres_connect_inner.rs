use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio::{sync::RwLock, time::error::Elapsed};
use tokio_postgres::Row;

use crate::{sql::SqlData, MyPostgresError, PostgresSettings};

pub struct PostgresConnectionSingleThreaded {
    postgres_client: Option<tokio_postgres::Client>,
    to_start: Option<Arc<PostgresConnectionInner>>,
}

impl PostgresConnectionSingleThreaded {
    pub fn new() -> Self {
        Self {
            postgres_client: None,
            to_start: None,
        }
    }

    pub fn new_connection(&mut self, client: tokio_postgres::Client) {
        self.postgres_client = Some(client);
    }

    pub fn disconnect(&mut self) {
        self.postgres_client = None;
    }

    pub fn get_connection(&self) -> Result<Option<&tokio_postgres::Client>, MyPostgresError> {
        if self.to_start.is_some() {
            return Ok(None);
        }

        if let Some(client) = &self.postgres_client {
            Ok(client.into())
        } else {
            Err(MyPostgresError::NoConnection)
        }
    }

    pub fn start_connection(&mut self) {
        if let Some(to_start) = self.to_start.take() {
            tokio::spawn(super::connection_loop::start_connection_loop(to_start));
        }
    }

    pub fn get_connection_mut(&mut self) -> Result<&mut tokio_postgres::Client, MyPostgresError> {
        if let Some(client) = &mut self.postgres_client {
            Ok(client.into())
        } else {
            Err(MyPostgresError::NoConnection)
        }
    }
}

pub struct PostgresConnectionInner {
    pub inner: Arc<RwLock<PostgresConnectionSingleThreaded>>,
    pub connected: Arc<AtomicBool>,
    pub sql_request_time_out: Duration,
    pub app_name: String,
    pub postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    pub to_be_disposable: AtomicBool,
    #[cfg(feature = "with-logs-and-telemetry")]
    pub logger: Arc<dyn rust_extensions::Logger + Send + Sync + 'static>,
}

impl PostgresConnectionInner {
    pub fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        sql_request_time_out: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<
            dyn rust_extensions::Logger + Send + Sync + 'static,
        >,
    ) -> Self {
        Self {
            app_name,
            postgres_settings,
            inner: Arc::new(RwLock::new(PostgresConnectionSingleThreaded::new())),
            connected: Arc::new(AtomicBool::new(false)),
            sql_request_time_out,
            to_be_disposable: AtomicBool::new(false),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        }
    }

    pub async fn engage(&self, to_start: Arc<PostgresConnectionInner>) {
        let mut write_access = self.inner.write().await;
        write_access.to_start = Some(to_start);
    }

    pub fn set_to_be_disposable(&self) {
        self.to_be_disposable.store(true, Ordering::Relaxed);
        self.disconnect();
    }

    pub fn is_to_be_disposable(&self) -> bool {
        self.to_be_disposable.load(Ordering::Relaxed)
    }

    pub fn disconnect(&self) {
        let tokio_postgres_client = self.inner.clone();

        let connected = self.connected.clone();

        tokio::spawn(async move {
            let mut write_access = tokio_postgres_client.write().await;
            write_access.disconnect();
            connected.store(false, std::sync::atomic::Ordering::SeqCst);
        });
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn handle_connection_is_established(
        &self,
        postgres_client: tokio_postgres::Client,
    ) -> DateTimeAsMicroseconds {
        let connected_date_time = DateTimeAsMicroseconds::now();
        println!(
            "{}: Postgres SQL Connection is established",
            connected_date_time.to_rfc3339()
        );

        {
            let mut write_access: tokio::sync::RwLockWriteGuard<
                '_,
                PostgresConnectionSingleThreaded,
            > = self.inner.write().await;
            write_access.new_connection(postgres_client);
        };
        self.connected.store(true, Ordering::Relaxed);

        connected_date_time
    }

    pub async fn execute_with_timeout<
        TResult,
        TFuture: Future<Output = Result<TResult, tokio_postgres::Error>>,
    >(
        &self,
        sql: Option<&str>,
        process_name: &str,
        execution: TFuture,
        #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<
            dyn rust_extensions::Logger + Send + Sync + 'static,
        >,
        #[cfg(feature = "with-logs-and-telemetry")] started: DateTimeAsMicroseconds,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
            &my_telemetry::MyTelemetryContext,
        >,
    ) -> Result<TResult, MyPostgresError> {
        let timeout_result: Result<Result<TResult, tokio_postgres::Error>, Elapsed> =
            tokio::time::timeout(self.sql_request_time_out, execution).await;

        let result = if timeout_result.is_err() {
            self.connected.store(false, Ordering::Relaxed);
            Err(MyPostgresError::TimeOut(self.sql_request_time_out))
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

    async fn start_connection(&self) {
        {
            let mut write_access = self.inner.write().await;
            write_access.start_connection();
        }

        loop {
            if self.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }

    pub async fn execute_sql(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
            &my_telemetry::MyTelemetryContext,
        >,
    ) -> Result<u64, MyPostgresError> {
        let mut start_connection = false;
        loop {
            if start_connection {
                self.start_connection().await;
            }
            let connection_access = self.inner.read().await;

            let connection_access = connection_access.get_connection()?;

            match connection_access {
                Some(connection_access) => {
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

                    let result = self
                        .execute_with_timeout(
                            Some(&sql.sql),
                            process_name,
                            execution,
                            #[cfg(feature = "with-logs-and-telemetry")]
                            &self.logger,
                            #[cfg(feature = "with-logs-and-telemetry")]
                            started,
                            #[cfg(feature = "with-logs-and-telemetry")]
                            telemetry_context,
                        )
                        .await;

                    return Ok(self.handle_error(result)?);
                }
                None => {
                    start_connection = true;
                }
            }
        }
    }

    pub async fn execute_sql_as_vec<'s>(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
            &my_telemetry::MyTelemetryContext,
        >,
    ) -> Result<Vec<Row>, MyPostgresError> {
        let mut start_connection = false;
        loop {
            if start_connection {
                self.start_connection().await;
            }

            let connection_access = self.inner.read().await;

            let connection_access = connection_access.get_connection()?;

            match connection_access {
                Some(connection_access) => {
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

                    let result = self
                        .execute_with_timeout(
                            Some(&sql.sql),
                            process_name,
                            execution,
                            #[cfg(feature = "with-logs-and-telemetry")]
                            &self.logger,
                            #[cfg(feature = "with-logs-and-telemetry")]
                            started,
                            #[cfg(feature = "with-logs-and-telemetry")]
                            telemetry_context,
                        )
                        .await;

                    return Ok(self.handle_error(result)?);
                }
                None => {
                    start_connection = true;
                }
            }
        }
    }

    pub async fn execute_bulk_sql<'s>(
        &self,
        sql_with_params: Vec<SqlData>,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
            &my_telemetry::MyTelemetryContext,
        >,
    ) -> Result<(), MyPostgresError> {
        if std::env::var("DEBUG").is_ok() {
            if let Some(first_value) = sql_with_params.get(0) {
                println!("SQL: {:?}", first_value.sql);
            }
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        let started = DateTimeAsMicroseconds::now();

        let mut connection_access = self.inner.write().await;

        let connection_access = connection_access.get_connection_mut()?;

        let execution = {
            let builder = connection_access.build_transaction();
            let transaction = builder.start().await?;

            for sql_data in &sql_with_params {
                transaction
                    .execute(&sql_data.sql, &sql_data.values.get_values_to_invoke())
                    .await?;
            }
            transaction.commit()
        };

        let result = self
            .execute_with_timeout(
                None,
                process_name,
                execution,
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
                MyPostgresError::ConnectionNotStartedYet => {}
            }
        }

        result
    }
}

#[cfg(feature = "with-logs-and-telemetry")]
async fn write_fail_telemetry_and_log(
    started: DateTimeAsMicroseconds,
    process: String,
    sql: Option<&str>,
    fail: String,
    telemetry_context: &my_telemetry::MyTelemetryContext,
    logger: &Arc<dyn rust_extensions::Logger + Send + Sync + 'static>,
) {
    let ctx = if let Some(sql) = sql {
        let mut ctx = std::collections::HashMap::new();
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
