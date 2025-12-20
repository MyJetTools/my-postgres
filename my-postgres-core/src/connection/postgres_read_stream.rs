use std::sync::{atomic::AtomicBool, Arc};

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
        ctx: crate::RequestContext,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(2048);
        let ctx_owned = ctx.to_owned();
        tokio::spawn(start_reading::<TEntity>(
            stream, tx, connected, sql, ctx_owned,
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

    pub async fn to_vec<TOut>(
        mut self,
        convert: impl Fn(TEntity) -> TOut,
    ) -> Result<Vec<TOut>, MyPostgresError> {
        let mut result = Vec::new();

        while let Some(item) = self.get_next().await? {
            result.push(convert(item));
        }

        Ok(result)
    }
}

async fn start_reading<TEntity: SelectEntity + Send + Sync + 'static>(
    stream: RowStream,
    sender: tokio::sync::mpsc::Sender<Result<TEntity, MyPostgresError>>,
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
                        "Ok reading stream".to_string(),
                        get_sql_telemetry_tags(Some(sql.as_str())),
                    )
                    .await;

                    break;
                }

                let row = row.unwrap();

                let entity = TEntity::from(&row);
                let _ = sender.send(Ok(entity)).await;
            }

            Err(e) => {
                {
                    ctx.write_fail(
                        format!("Pulled {} ok rows before error: {:?}", read_ok_rows, e),
                        Some(sql.as_str()),
                        #[cfg(feature = "with-logs-and-telemetry")]
                        get_sql_telemetry_tags(Some(sql.as_str())),
                    )
                    .await;
                }

                connected.store(false, std::sync::atomic::Ordering::Relaxed);
                sender.send(Err(e.into())).await.unwrap();
                break;
            }
        }
    }
}
