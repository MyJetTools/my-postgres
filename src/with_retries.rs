use std::{sync::Arc, time::Duration};

#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use crate::{
    count_result::CountResult, sql_insert::SqlInsertModel, sql_select::SelectEntity,
    sql_where::SqlWhereModel, MyPostgresError, PostgresConnection,
};

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

    pub async fn get_count<TWhereModel: SqlWhereModel, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgresError> {
        let mut attempt_no = 0;
        loop {
            let result = self
                .connection
                .get_count(
                    table_name,
                    where_model,
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

    pub async fn query_single_row<TEntity: SelectEntity, TWhereModel: SqlWhereModel>(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let mut attempt_no = 0;
        loop {
            let result = self
                .connection
                .query_single_row(
                    table_name,
                    where_model,
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

    pub async fn query_single_row_with_processing<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        post_processing: &TPostProcessing,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let mut attempt_no = 0;
        loop {
            let result = self
                .connection
                .query_single_row_with_processing(
                    table_name,
                    where_model,
                    post_processing,
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
