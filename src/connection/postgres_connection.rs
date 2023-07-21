use std::{sync::Arc, time::Duration};

#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use rust_extensions::StrOrString;
use tokio_postgres::Row;

use crate::{
    sql::SqlData, ConnectionString, ConnectionsPool, MyPostgresError, PostgresConnectionInstance,
    PostgresSettings,
};

pub enum PostgresConnection {
    Single(PostgresConnectionInstance),
    Pool(ConnectionsPool),
}

impl PostgresConnection {
    pub async fn new_as_single_connection(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let app_name: StrOrString<'static> = app_name.into();
        let connection = PostgresConnectionInstance::new(
            app_name,
            postgres_settings,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        )
        .await;

        Self::Single(connection)
    }

    pub fn new_as_multiple_connections(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        sql_request_timeout: Duration,
        max_pool_size: usize,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let app_name: StrOrString<'static> = app_name.into();
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
        sql: &SqlData,
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
        sql_with_params: Vec<SqlData>,
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

    pub async fn get_connection_string(&self) -> (String, ConnectionString) {
        match self {
            PostgresConnection::Single(connection) => {
                let conn_string = connection
                    .get_postgres_settings()
                    .get_connection_string()
                    .await;

                let conn_string_format =
                    crate::ConnectionStringFormat::parse_and_detect(conn_string.as_str());

                (
                    connection.get_app_name().to_string(),
                    ConnectionString::parse(conn_string_format),
                )
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                let conn_string = connection
                    .as_ref()
                    .get_postgres_settings()
                    .get_connection_string()
                    .await;

                let conn_string_format =
                    crate::ConnectionStringFormat::parse_and_detect(conn_string.as_str());

                (
                    connection.as_ref().get_app_name().to_string(),
                    ConnectionString::parse(conn_string_format),
                )
            }
        }
    }
    pub async fn get_db_name(&self) -> String {
        match self {
            PostgresConnection::Single(connection) => connection.get_db_name().await,
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection.as_ref().get_db_name().await
            }
        }
    }

    pub async fn execute_sql_as_vec<TEntity, TTransform: Fn(&Row) -> TEntity>(
        &self,
        sql: &SqlData,
        process_name: Option<&str>,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection
                    .execute_sql_as_vec(
                        &sql,
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

    #[cfg(feature = "with-logs-and-telemetry")]
    pub fn get_logger(&self) -> &Arc<dyn Logger + Sync + Send + 'static> {
        match self {
            PostgresConnection::Single(connection) => &connection.logger,
            PostgresConnection::Pool(pool) => &pool.logger,
        }
    }
}
