#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use crate::is_debug;
use rust_extensions::StrOrString;
use std::{sync::Arc, time::Duration};

use crate::{
    count_result::CountResult,
    sql::SqlData,
    sql_insert::SqlInsertModel,
    sql_select::{BulkSelectBuilder, BulkSelectEntity, SelectEntity},
    sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel,
    union::UnionModel,
    MyPostgresBuilder, MyPostgresError, PostgresConnection, PostgresReadStream, PostgresSettings,
    SqlOperationWithRetries, UpdateConflictType,
};

pub struct MyPostgres {
    connection: Arc<PostgresConnection>,
    sql_request_timeout: Duration,
}

#[derive(Debug)]
pub enum ConcurrentOperationResult<TModel> {
    Created(TModel),
    CreatedCanceled,
    Updated(TModel),
    UpdateCanceledOnModel(TModel),
}

impl MyPostgres {
    pub fn from_settings(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    ) -> MyPostgresBuilder {
        MyPostgresBuilder::new(app_name, postgres_settings)
    }

    pub fn from_connection_string(connection: Arc<PostgresConnection>) -> MyPostgresBuilder {
        MyPostgresBuilder::from_connection(connection)
    }

    pub fn create(connection: Arc<PostgresConnection>, sql_request_timeout: Duration) -> Self {
        println!(
            "Created connection with sql_timeout: {:?}",
            sql_request_timeout
        );
        Self {
            connection,
            sql_request_timeout,
        }
    }

    pub async fn get_count<TWhereModel: SqlWhereModel + std::fmt::Debug, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgresError> {
        self.connection
            .get_count(
                table_name,
                where_model,
                self.sql_request_timeout,
                is_debug(table_name, "get_count"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_single_row<
        TEntity: SelectEntity,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        self.connection
            .query_single_row(
                table_name,
                where_model,
                self.sql_request_timeout,
                is_debug(table_name, "query_single_row"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_single_row_with_processing<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        post_processing: TPostProcessing,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        self.connection
            .query_single_row_with_processing(
                table_name,
                where_model,
                &post_processing,
                self.sql_request_timeout,
                is_debug(table_name, "query_single_row_with_processing"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_sql(
        &self,
        sql: SqlData,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let ctx = crate::RequestContext::new(
            self.sql_request_timeout,
            "execute_sql".to_string(),
            is_debug(sql.sql.as_str(), "execute_sql"),
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        self.connection.execute_sql(&sql, &ctx).await
    }

    pub async fn execute_sql_as_vec<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        sql: SqlData,
        ctx: &crate::RequestContext,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        self.connection
            .execute_sql_as_vec(&sql, |row| TEntity::from(row), ctx)
            .await
    }

    pub async fn query_rows_as_stream<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<PostgresReadStream<TEntity>, MyPostgresError> {
        self.connection
            .query_rows_as_stream(
                table_name,
                where_model,
                self.sql_request_timeout,
                is_debug(table_name, "query_rows_as_stream"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        self.connection
            .query_rows(
                table_name,
                where_model,
                self.sql_request_timeout,
                is_debug(table_name, "query_rows"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows_with_processing<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        post_processing: TPostProcessing,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        self.connection
            .query_rows_with_processing(
                table_name,
                where_model,
                &post_processing,
                self.sql_request_timeout,
                is_debug(table_name, "query_rows_with_processing"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_query_rows_with_transformation<
        TIn: SqlWhereModel + Send + Sync + 'static + 'static + std::fmt::Debug,
        TOut,
        TEntity: SelectEntity + BulkSelectEntity + Send + Sync + 'static,
        TTransform: Fn(&TIn, Option<TEntity>) -> TOut,
    >(
        &self,
        sql_builder: &BulkSelectBuilder<TIn>,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TOut>, MyPostgresError> {
        self.connection
            .bulk_query_rows_with_transformation(
                sql_builder,
                &transform,
                self.sql_request_timeout,
                is_debug(
                    sql_builder.table_name,
                    "bulk_query_rows_with_transformation",
                ),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_db_entity<TEntity: SqlInsertModel + std::fmt::Debug>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.connection
            .insert_db_entity(
                entity,
                table_name,
                self.sql_request_timeout,
                is_debug(table_name, "insert_db_entity"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_db_entity_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.connection
            .insert_db_entity_if_not_exists(
                entity,
                table_name,
                self.sql_request_timeout,
                is_debug(table_name, "insert_db_entity_if_not_exists"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_db_entities<TEntity: SqlInsertModel + 'static + std::fmt::Debug>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        if entities.len() == 0 {
            panic!("Attempt to bulk_insert_db_entities 0 entities");
        }
        self.connection
            .bulk_insert_db_entities(
                entities,
                table_name,
                self.sql_request_timeout,
                is_debug(table_name, "bulk_insert_db_entities"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_db_entities_if_not_exists<
        TEntity: SqlInsertModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        entities: &[TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .bulk_insert_db_entities_if_not_exists(
                table_name,
                entities,
                self.sql_request_timeout,
                is_debug(table_name, "bulk_insert_db_entities_if_not_exists"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn update_db_entity<'s, TEntity: SqlUpdateModel + SqlWhereModel + std::fmt::Debug>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.connection
            .update_db_entity(
                entity,
                table_name,
                self.sql_request_timeout,
                is_debug(table_name, "update_db_entity"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_query<TEntity: SelectEntity, TWhereModel: SqlWhereModel + std::fmt::Debug>(
        &self,
        table_name: &str,
        where_models: Vec<TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<UnionModel<TEntity, TWhereModel>>, MyPostgresError> {
        self.connection
            .bulk_query(
                table_name,
                where_models,
                self.sql_request_timeout,
                is_debug(table_name, "bulk_query"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_query_with_transformation<
        TEntity: SelectEntity,
        TOut,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        transformation: impl Fn(TEntity) -> TOut,
        where_models: Vec<TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<UnionModel<TOut, TWhereModel>>, MyPostgresError> {
        self.connection
            .bulk_query_with_transformation(
                table_name,
                where_models,
                &transformation,
                self.sql_request_timeout,
                is_debug(table_name, "bulk_query_with_transformation"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel + SqlUpdateModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        update_conflict_type: UpdateConflictType<'s>,
        entities: &[TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        if entities.len() == 0 {
            return Err(MyPostgresError::Other(
                "Attempt to bulk_insert_or_update_db_entity 0 entities".to_string(),
            ));
        }

        self.connection
            .bulk_insert_or_update_db_entity(
                table_name,
                &update_conflict_type,
                entities,
                self.sql_request_timeout,
                is_debug(table_name, "bulk_insert_or_update_db_entity"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel + SqlUpdateModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        update_conflict_type: UpdateConflictType<'s>,
        entity: &TEntity,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .insert_or_update_db_entity(
                table_name,
                &update_conflict_type,
                entity,
                self.sql_request_timeout,
                is_debug(table_name, "insert_or_update_db_entity"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    #[deprecated(note = "Please use delete instead")]
    pub async fn delete_db_entity<TWhereModel: SqlWhereModel + std::fmt::Debug>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .delete(
                table_name,
                where_model,
                self.sql_request_timeout,
                is_debug(table_name, "delete_db_entity"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn delete<TWhereModel: SqlWhereModel + std::fmt::Debug>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .delete(
                table_name,
                where_model,
                self.sql_request_timeout,
                is_debug(table_name, "delete"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_delete<TEntity: SqlWhereModel + std::fmt::Debug>(
        &self,
        table_name: &str,
        entities: &[TEntity],

        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .bulk_delete(
                table_name,
                entities,
                self.sql_request_timeout,
                is_debug(table_name, "bulk_delete"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn concurrent_insert_or_update_single_entity<
        's,
        TModel: SelectEntity + SqlInsertModel + SqlUpdateModel + SqlWhereModel + std::fmt::Debug,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        crate_new_model: impl Fn() -> Option<TModel>,
        update_model: impl Fn(&mut TModel) -> bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<ConcurrentOperationResult<TModel>, MyPostgresError> {
        self.connection
            .concurrent_insert_or_update_single_entity(
                table_name,
                where_model,
                &crate_new_model,
                &update_model,
                self.sql_request_timeout,
                is_debug(table_name, "concurrent_insert_or_update_single_entity"),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub fn with_retries(
        &self,
        retries: usize,
        delay_between_retries: Duration,
    ) -> SqlOperationWithRetries {
        SqlOperationWithRetries::new(
            self.connection.clone(),
            delay_between_retries,
            retries,
            self.sql_request_timeout,
        )
    }
}
