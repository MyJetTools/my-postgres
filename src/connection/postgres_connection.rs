use std::{sync::Arc, time::Duration};

#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use tokio_postgres::Row;

use crate::{
    ConnectionsPool, MyPostgressError, PostgresConnectionInstance, PostgressSettings, SqlValue,
};

pub enum PostgresConnection {
    Single(PostgresConnectionInstance),
    Pool(ConnectionsPool),
}

impl PostgresConnection {
    pub fn new_as_single_connection(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let connection = PostgresConnectionInstance::new(
            app_name,
            postgres_settings,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        );

        Self::Single(connection)
    }

    pub fn new_as_multiple_connections(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        sql_request_timeout: Duration,
        max_pool_size: usize,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self::Pool(ConnectionsPool::new(
            app_name,
            postgres_settings,
            max_pool_size,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        ))
    }
    pub async fn execute_sql(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgressError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection
                    .execute_sql(
                        sql,
                        params,
                        process_name,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection
                    .as_ref()
                    .execute_sql(
                        sql,
                        params,
                        process_name,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            }
        }
    }

    pub async fn execute_bulk_sql<'s>(
        &self,
        sql_with_params: Vec<(String, Vec<SqlValue<'s>>)>,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection
                    .execute_bulk_sql(
                        sql_with_params,
                        process_name,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection
                    .as_ref()
                    .execute_bulk_sql(
                        sql_with_params,
                        process_name,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            }
        }
    }

    pub async fn execute_sql_as_vec<TEntity, TTransform: Fn(&Row) -> TEntity>(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        process_name: &str,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection
                    .execute_sql_as_vec(
                        sql,
                        params,
                        process_name,
                        transform,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection
                    .as_ref()
                    .execute_sql_as_vec(
                        sql,
                        params,
                        process_name,
                        transform,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            }
        }
    }
}
