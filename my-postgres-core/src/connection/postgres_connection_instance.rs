#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use tokio_postgres::Row;

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
}

impl PostgresConnectionInstance {
    pub async fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        let inner = Arc::new(PostgresConnectionInner::new(
            app_name,
            postgres_settings,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger.clone(),
        ));

        let result = Self {
            inner,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
            created: DateTimeAsMicroseconds::now(),
        };

        result.inner.engage(result.inner.clone()).await;

        result
    }

    pub fn get_postgres_settings(&self) -> &Arc<dyn PostgresSettings + Sync + Send + 'static> {
        &self.inner.postgres_settings
    }

    pub fn get_app_name(&self) -> &str {
        self.inner.app_name.as_str()
    }

    pub async fn await_until_connected(&self) {
        loop {
            if self.inner.is_connected() {
                break;
            }

            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }

    pub fn disconnect(&self) {
        self.inner.disconnect();
    }

    pub fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    pub async fn get_db_name(&self) -> String {
        let conn_string = self.inner.postgres_settings.get_connection_string().await;

        let conn_string_format =
            crate::ConnectionStringFormat::parse_and_detect(conn_string.as_str());

        let connection_string = ConnectionString::parse(conn_string_format);

        connection_string.get_db_name().to_string()
    }

    pub async fn execute_sql(
        &self,
        sql: &SqlData,
        process_name: String,
        sql_request_time_out: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.inner
            .execute_sql(
                sql,
                process_name,
                sql_request_time_out,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_bulk_sql<'s>(
        &self,
        sql_with_params: Vec<SqlData>,
        process_name: String,
        sql_request_time_out: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.inner
            .execute_bulk_sql(
                sql_with_params,
                process_name,
                sql_request_time_out,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_sql_as_vec<'s, TEntity, TTransform: Fn(&Row) -> TEntity>(
        &self,
        sql: &SqlData,
        process_name: String,
        transform: TTransform,
        sql_request_time_out: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let result_rows_set = self
            .inner
            .execute_sql_as_vec(
                sql,
                process_name,
                sql_request_time_out,
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
        process_name: String,
        sql_request_time_out: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TCountResult>, MyPostgresError> {
        let mut result = self
            .execute_sql_as_vec(
                sql,
                process_name,
                |db_row| TCountResult::from_db_row(db_row),
                sql_request_time_out,
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

impl Drop for PostgresConnectionInstance {
    fn drop(&mut self) {
        self.inner.set_to_be_disposable();
    }
}
