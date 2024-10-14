use std::sync::{atomic::AtomicBool, Arc};

use tokio_postgres::RowStream;

use futures_util::{pin_mut, TryStreamExt};

use crate::{DbRow, MyPostgresError};

pub struct PostgresRowReadStream {
    rx: tokio::sync::mpsc::Receiver<Result<DbRow, MyPostgresError>>,
}

impl PostgresRowReadStream {
    pub fn new(
        sql: String,
        stream: RowStream,
        connected: Arc<AtomicBool>,
        ctx: crate::RequestContext,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(2048);
        tokio::spawn(start_reading(stream, tx, connected, sql, ctx));
        Self { rx }
    }

    pub async fn get_next(&mut self) -> Result<Option<DbRow>, MyPostgresError> {
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

async fn start_reading(
    stream: RowStream,
    sender: tokio::sync::mpsc::Sender<Result<DbRow, MyPostgresError>>,
    connected: Arc<AtomicBool>,
    sql: String,
    ctx: crate::RequestContext,
) {
    #[cfg(feature = "with-logs-and-telemetry")]
    use crate::connection::get_sql_telemetry_tags;

    pin_mut!(stream);

    let mut read_ok_rows = 0;

    loop {
        let next_future = stream.try_next();

        let read_result = tokio::time::timeout(ctx.sql_request_time_out, next_future).await;

        if read_result.is_err() {
            connected.store(false, std::sync::atomic::Ordering::Relaxed);
            sender
                .send(Err(MyPostgresError::TimeOut(ctx.sql_request_time_out)))
                .await
                .unwrap();
            break;
        }

        match read_result.unwrap() {
            Ok(row) => {
                read_ok_rows += 1;
                if row.is_none() {
                    #[cfg(feature = "with-logs-and-telemetry")]
                    ctx.write_success(
                        format!("Read {} records from stream", read_ok_rows),
                        get_sql_telemetry_tags(Some(sql.as_str())),
                    )
                    .await;
                    break;
                }

                let row = row.unwrap();
                let _ = sender.send(Ok(row)).await;
            }

            Err(e) => {
                // let process = "Reading sql request as stream".to_string();
                // let error_message = format!("Pulled {} ok rows before error: {:?}",read_ok_rows, e);

                ctx.write_fail(
                    format!("Error reading from postgres stream. Err:{:?}", e),
                    Some(&sql),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    get_sql_telemetry_tags(Some(sql.as_str())),
                )
                .await;

                /*
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
                 */

                connected.store(false, std::sync::atomic::Ordering::Relaxed);
                sender.send(Err(e.into())).await.unwrap();
                break;
            }
        }
    }
}
