use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use tokio_postgres::RowStream;

use futures_util::{pin_mut, TryStreamExt};

use crate::{sql_select::SelectEntity, MyPostgresError};

pub struct PostgresReadStream<TEntity: SelectEntity + Send + Sync + 'static> {
    rx: tokio::sync::mpsc::Receiver<Result<TEntity, MyPostgresError>>,
}

impl<TEntity: SelectEntity + Send + Sync + 'static> PostgresReadStream<TEntity> {
    pub fn new(
        sql: String,
        stream: RowStream,
        connected: Arc<AtomicBool>,
        request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<
            dyn rust_extensions::Logger + Send + Sync + 'static,
        >,
        #[cfg(feature = "with-logs-and-telemetry")]
        started: rust_extensions::date_time::DateTimeAsMicroseconds,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
            &my_telemetry::MyTelemetryContext,
        >,
    ) -> Self {
        #[cfg(feature = "with-logs-and-telemetry")]
        let telemetry_context = telemetry_context.cloned();

        #[cfg(feature = "with-logs-and-telemetry")]
        let logger = logger.clone();

        let (tx, rx) = tokio::sync::mpsc::channel(2048);
        tokio::spawn(start_reading::<TEntity>(
            stream,
            tx,
            connected,
            request_timeout,
            sql,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
            #[cfg(feature = "with-logs-and-telemetry")]
            started,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        ));
        Self { rx }
    }

    pub async fn get_next(&mut self) -> Result<Option<TEntity>, MyPostgresError> {
        let result = self.rx.recv().await;

        match result {
            Some(result) => {
                let result = result?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

async fn start_reading<TEntity: SelectEntity + Send + Sync + 'static>(
    stream: RowStream,
    sender: tokio::sync::mpsc::Sender<Result<TEntity, MyPostgresError>>,
    connected: Arc<AtomicBool>,
    request_timeout: Duration,
    sql: String,
    #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<
        dyn rust_extensions::Logger + Send + Sync + 'static,
    >,
    #[cfg(feature = "with-logs-and-telemetry")]
    started: rust_extensions::date_time::DateTimeAsMicroseconds,
    #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
        my_telemetry::MyTelemetryContext,
    >,
) {
    #[cfg(feature = "with-logs-and-telemetry")]
    use crate::connection::get_sql_telemetry_tags;

    pin_mut!(stream);

    loop {
        let next_future = stream.try_next();

        let read_result = tokio::time::timeout(request_timeout, next_future).await;

        if read_result.is_err() {
            connected.store(false, std::sync::atomic::Ordering::Relaxed);
            sender
                .send(Err(MyPostgresError::TimeOut(request_timeout)))
                .await
                .unwrap();
            break;
        }

        match stream.try_next().await {
            Ok(row) => {
                if row.is_none() {
                    #[cfg(feature = "with-logs-and-telemetry")]
                    if let Some(my_telemetry) = telemetry_context.as_ref() {
                        my_telemetry::TELEMETRY_INTERFACE
                            .write_success(
                                my_telemetry,
                                started,
                                "Reading sql request as stream".to_string(),
                                "Ok".to_string(),
                                get_sql_telemetry_tags(Some(sql.as_str())),
                            )
                            .await;
                    }

                    break;
                }

                let row = row.unwrap();

                let entity = TEntity::from(&row);
                let _ = sender.send(Ok(entity)).await;
            }

            Err(e) => {
                #[cfg(feature = "with-logs-and-telemetry")]
                {
                    let process = "Reading sql request as stream".to_string();
                    let error_message = format!("{:?}", e);
                    if let Some(my_telemetry) = telemetry_context.as_ref() {
                        my_telemetry::TELEMETRY_INTERFACE
                            .write_fail(
                                my_telemetry,
                                started,
                                process.to_string(),
                                error_message.to_string(),
                                get_sql_telemetry_tags(Some(sql.as_str())),
                            )
                            .await;
                    }

                    let mut ctx = std::collections::HashMap::new();
                    ctx.insert("sql".to_string(), sql.to_string());

                    logger.write_error(process, error_message, Some(ctx));
                }

                #[cfg(not(feature = "with-logs-and-telemetry"))]
                println!(
                    "Error reading sql request as stream. Err: {:?}. Sql: {}",
                    e, sql
                );

                connected.store(false, std::sync::atomic::Ordering::Relaxed);
                sender.send(Err(e.into())).await.unwrap();
                break;
            }
        }
    }
}
