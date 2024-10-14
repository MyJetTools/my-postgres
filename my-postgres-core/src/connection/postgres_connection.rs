use std::sync::Arc;

use rust_extensions::StrOrString;
use tokio_postgres::Row;

use crate::{
    sql::SqlData, sql_select::SelectEntity, ConnectionsPool, MyPostgresError,
    PostgresConnectionInstance, PostgresConnectionString, PostgresSettings, RequestContext,
};

use super::{PostgresReadStream, PostgresRowReadStream};

pub enum PostgresConnection {
    Single(PostgresConnectionInstance),
    Pool(ConnectionsPool),
}

impl PostgresConnection {
    pub async fn new_as_single_connection(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-ssh")] ssh_config_builder: Option<crate::ssh::SshConfigBuilder>,
    ) -> Self {
        let app_name: StrOrString<'static> = app_name.into();

        let conn_string = postgres_settings.get_connection_string().await;
        let conn_string = PostgresConnectionString::from_str(conn_string.as_str());

        let connection = PostgresConnectionInstance::new(
            app_name.to_string(),
            conn_string.get_db_name().to_string(),
            postgres_settings,
            #[cfg(feature = "with-ssh")]
            conn_string.get_ssh_config(ssh_config_builder),
        )
        .await;

        Self::Single(connection)
    }

    #[cfg(feature = "with-ssh")]
    pub fn get_ssh_config(&self) -> Option<crate::ssh::PostgresSshConfig> {
        match self {
            PostgresConnection::Single(connection) => connection.ssh_config.clone(),
            PostgresConnection::Pool(pool) => pool.ssh_config.clone(),
        }
    }

    pub async fn new_as_multiple_connections(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        max_pool_size: usize,
        #[cfg(feature = "with-ssh")] ssh_config_builder: Option<crate::ssh::SshConfigBuilder>,
    ) -> Self {
        let app_name: StrOrString<'static> = app_name.into();
        let conn_string = postgres_settings.get_connection_string().await;
        let conn_string = PostgresConnectionString::from_str(conn_string.as_str());

        Self::Pool(ConnectionsPool::new(
            app_name,
            conn_string.get_db_name().to_string(),
            postgres_settings,
            max_pool_size,
            #[cfg(feature = "with-ssh")]
            conn_string.get_ssh_config(ssh_config_builder),
        ))
    }

    pub async fn execute_sql(
        &self,
        sql: &SqlData,
        ctx: &RequestContext,
    ) -> Result<u64, MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => connection.execute_sql(sql, ctx).await,
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection.as_ref().execute_sql(sql, ctx).await
            }
        }
    }

    pub async fn execute_bulk_sql(
        &self,
        sql_with_params: Vec<SqlData>,
        ctx: RequestContext,
    ) -> Result<(), MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection.execute_bulk_sql(sql_with_params, ctx).await
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection
                    .as_ref()
                    .execute_bulk_sql(sql_with_params, ctx)
                    .await
            }
        }
    }

    pub async fn get_connection_string(&self) -> (String, PostgresConnectionString) {
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
                    PostgresConnectionString::parse(conn_string_format),
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
                    PostgresConnectionString::parse(conn_string_format),
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
        transform: TTransform,
        ctx: &crate::RequestContext,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection.execute_sql_as_vec(&sql, transform, ctx).await
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection
                    .as_ref()
                    .execute_sql_as_vec(sql, transform, ctx)
                    .await
            }
        }
    }

    pub async fn execute_sql_as_stream<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        sql: &SqlData,
        ctx: crate::RequestContext,
    ) -> Result<PostgresReadStream<TEntity>, MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection.execute_sql_as_stream(sql, ctx).await
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection.as_ref().execute_sql_as_stream(sql, ctx).await
            }
        }
    }

    pub async fn execute_sql_as_row_stream(
        &self,
        sql: &SqlData,
        ctx: &crate::RequestContext,
    ) -> Result<PostgresRowReadStream, MyPostgresError> {
        match self {
            PostgresConnection::Single(connection) => {
                connection.execute_sql_as_row_stream(&sql, ctx).await
            }
            PostgresConnection::Pool(pool) => {
                let connection = pool.get().await;
                connection
                    .as_ref()
                    .execute_sql_as_row_stream(sql, ctx)
                    .await
            }
        }
    }
}
