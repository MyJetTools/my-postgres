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
    pub fn new(stream: RowStream, connected: Arc<AtomicBool>, request_timeout: Duration) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(2048);
        tokio::spawn(start_reading::<TEntity>(
            stream,
            tx,
            connected,
            request_timeout,
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
) {
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
                    break;
                }

                let row = row.unwrap();

                let entity = TEntity::from(&row);
                let _ = sender.send(Ok(entity)).await;
            }

            Err(e) => {
                connected.store(false, std::sync::atomic::Ordering::Relaxed);
                sender.send(Err(e.into())).await.unwrap();
                break;
            }
        }
    }
}
