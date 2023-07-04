use std::{sync::Arc, time::Duration};

use crate::{sql_insert::SqlInsertModel, MyPostgresError, PostgresConnection};

pub struct SqlOperationWithRetries {
    connection: Arc<PostgresConnection>,
    delay: Duration,
    retries_amount: usize,
}

impl SqlOperationWithRetries {
    pub fn new(
        connection: Arc<PostgresConnection>,
        delay: Duration,
        retries_amount: usize,
    ) -> Self {
        Self {
            connection,
            delay,
            retries_amount,
        }
    }

    pub async fn insert_db_entity_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
            &my_telemetry::MyTelemetryContext,
        >,
    ) -> Result<u64, MyPostgresError> {
        let mut attempt_no = 0;

        loop {
            let result = self
                .connection
                .insert_db_entity_if_not_exists(
                    entity,
                    table_name,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                )
                .await;

            match result {
                Ok(result) => return Ok(result),
                Err(err) => {
                    self.handle_error(err, attempt_no).await?;
                    attempt_no += 1;
                }
            }
        }
    }

    async fn handle_error(
        &self,
        err: MyPostgresError,
        attempt_no: usize,
    ) -> Result<(), MyPostgresError> {
        if attempt_no >= self.retries_amount {
            return Err(err);
        }

        tokio::time::sleep(self.delay).await;
        Ok(())
    }
}
