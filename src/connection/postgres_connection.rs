use std::{sync::Arc, time::Duration};

#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use rust_extensions::StrOrString;
use tokio_postgres::Row;

use crate::{
    sql::{SqlData, SqlValues},
    ConnectionsPool, MyPostgresError, PostgresConnectionInstance, PostgresSettings,
};

pub enum PostgresConnection {
    Single(PostgresConnectionInstance),
    Pool(ConnectionsPool),
}

impl PostgresConnection {
    pub fn new_as_single_connection(
        app_name: StrOrString<'static>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
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
        app_name: StrOrString<'static>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
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
        sql: SqlData,
        process_name: Option<&str>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection
                    .execute_sql(
                        sql,
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
                        process_name,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            }
        }
    }

    pub async fn execute_bulk_sql(
        &self,
        sql_with_params: Vec<(String, SqlValues)>,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
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
        sql: SqlData,
        process_name: Option<&str>,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection
                    .execute_sql_as_vec(
                        sql,
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
