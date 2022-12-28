#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
use rust_extensions::date_time::DateTimeAsMicroseconds;
#[cfg(feature = "with-logs-and-telemetry")]
use std::collections::HashMap;
use tokio::time::error::Elapsed;
use tokio_postgres::Row;

#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::{
    future::Future,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use crate::{MyPostgressError, SqlValueToWrite};

pub struct PostgresConnection {
    client: tokio_postgres::Client,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Send + Sync + 'static>,
    pub connected: Arc<AtomicBool>,
    pub created: DateTimeAsMicroseconds,
    pub sql_request_timeout: Duration,
}

impl PostgresConnection {
    pub fn new(
        client: tokio_postgres::Client,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        Self {
            client: client,
            connected: Arc::new(AtomicBool::new(true)),
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
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgressError> {
        if std::env::var("DEBUG").is_ok() {
            println!("SQL: {}", sql);
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        let started = DateTimeAsMicroseconds::now();

        let execution = self.client.execute(sql, params);

        let result = execute_with_timeout(
            process_name,
            Some(sql),
            execution,
            self.sql_request_timeout,
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
        &mut self,
        sql_with_params: Vec<(String, Vec<SqlValueToWrite<'s>>)>,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        if std::env::var("DEBUG").is_ok() {
            println!("SQL: {:?}", sql_with_params);
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        let started = DateTimeAsMicroseconds::now();

        let execution = {
            let builder = self.client.build_transaction();
            let transaction = builder.start().await?;

            for (sql, params) in &sql_with_params {
                let mut params_to_invoke = Vec::with_capacity(params.len());

                for param in params {
                    params_to_invoke.push(param.get_value());
                }

                transaction.execute(sql, &params_to_invoke).await?;
            }
            transaction.commit()
        };

        let result = execute_with_timeout(
            process_name,
            None,
            execution,
            self.sql_request_timeout,
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

    pub async fn execute_sql_as_vec<TEntity, TTransform: Fn(&Row) -> TEntity>(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        process_name: &str,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        if std::env::var("DEBUG").is_ok() {
            println!("SQL: {}", sql);
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        let started = DateTimeAsMicroseconds::now();

        let execution = self.client.query(sql, params);

        let result = execute_with_timeout(
            process_name,
            Some(sql),
            execution,
            self.sql_request_timeout,
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
        result: Result<TResult, MyPostgressError>,
    ) -> Result<TResult, MyPostgressError> {
        if let Err(err) = &result {
            match err {
                MyPostgressError::NoConnection => {}
                MyPostgressError::SingleRowRequestReturnedMultipleRows(_) => {}
                MyPostgressError::PostgresError(_) => {}
                MyPostgressError::Other(_) => {
                    self.disconnect();
                }
                MyPostgressError::Timeouted(_) => {
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
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Send + Sync + 'static>,
    #[cfg(feature = "with-logs-and-telemetry")] started: DateTimeAsMicroseconds,
    #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
) -> Result<TResult, MyPostgressError> {
    let timeout_result: Result<Result<TResult, tokio_postgres::Error>, Elapsed> =
        tokio::time::timeout(sql_request_timeout, execution).await;

    let result = if timeout_result.is_err() {
        Err(MyPostgressError::Timeouted(sql_request_timeout))
    } else {
        match timeout_result.unwrap() {
            Ok(result) => Ok(result),
            Err(err) => Err(MyPostgressError::PostgresError(err)),
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
