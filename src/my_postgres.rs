#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use rust_extensions::{date_time::DateTimeAsMicroseconds, StrOrString};
use std::{sync::Arc, time::Duration};

use crate::{
    count_result::CountResult,
    sql::SqlData,
    sql_insert::SqlInsertModel,
    sql_select::{BulkSelectBuilder, BulkSelectEntity, SelectEntity, ToSqlString},
    sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel,
    table_schema::{PrimaryKeySchema, TableSchema, TableSchemaProvider},
    MyPostgresError, PostgresConnection, PostgresConnectionInstance, PostgresSettings,
    SqlOperationWithRetries, UpdateConflictType,
};

pub struct MyPostgres {
    connection: Arc<PostgresConnection>,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Sync + Send + 'static>,
}

pub enum ConcurrentOperationResult<TModel> {
    Created(TModel),
    CreatedCanceled,
    Updated(TModel),
    UpdateCanceledOnModel(TModel),
}

impl MyPostgres {
    pub fn new(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let app_name: StrOrString<'static> = app_name.into();
        let connection = PostgresConnectionInstance::new(
            app_name,
            postgres_settings,
            Duration::from_secs(5),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger.clone(),
        );

        Self {
            connection: Arc::new(PostgresConnection::Single(connection)),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        }
    }

    pub fn with_shared_connection(
        connection: Arc<PostgresConnection>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            connection,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        }
    }

    pub async fn check_table_schema<TTableSchemaProvider: TableSchemaProvider>(
        &self,
        table_name: &'static str,
        primary_key_name: Option<String>,
    ) {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let columns = TTableSchemaProvider::get_columns();

        let primary_key = if let Some(primary_key_name) = primary_key_name {
            if let Some(primary_key_columns) = TTableSchemaProvider::PRIMARY_KEY_COLUMNS {
                Some((
                    primary_key_name,
                    PrimaryKeySchema::from_vec_of_str(primary_key_columns),
                ))
            } else {
                panic!(
                    "Provided primary key name {}, but there are no primary key columns defined.",
                    primary_key_name
                )
            }
        } else {
            None
        };

        let indexes = TTableSchemaProvider::get_indexes();

        let table_schema = TableSchema::new(table_name, primary_key, columns, indexes);

        let started = DateTimeAsMicroseconds::now();

        while let Err(err) = crate::sync_table_schema::sync_schema(
            &self.connection,
            &table_schema,
            #[cfg(feature = "with-logs-and-telemetry")]
            &self.logger,
        )
        .await
        {
            println!(
                "Can not verify schema for table {} because of error {:?}",
                table_name, err
            );

            if DateTimeAsMicroseconds::now()
                .duration_since(started)
                .as_positive_or_zero()
                > Duration::from_secs(20)
            {
                panic!(
                    "Aborting  the process due to the failing to verify table {} schema during 20 seconds.",
                    table_name
                );
            } else {
                println!("Retrying in 3 seconds...");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }

    pub async fn with_table_schema_verification<TTableSchemaProvider: TableSchemaProvider>(
        self,
        table_name: &'static str,
        primary_key_name: Option<String>,
    ) -> Self {
        self.check_table_schema::<TTableSchemaProvider>(table_name, primary_key_name)
            .await;
        self
    }

    pub async fn get_count<TWhereModel: SqlWhereModel, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgresError> {
        self.connection
            .get_count(
                table_name,
                where_model,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_single_row<TEntity: SelectEntity, TWhereModel: SqlWhereModel>(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        self.connection
            .query_single_row(
                table_name,
                where_model,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_single_row_with_processing<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
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
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_sql<ToSql: ToSqlString>(
        &self,
        sql: SqlData,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.connection
            .execute_sql(
                &sql,
                None,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_sql_as_vec<
        ToSql: ToSqlString,
        TEntity: SelectEntity + Send + Sync + 'static,
    >(
        &self,
        sql: SqlData,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        self.connection
            .execute_sql_as_vec(
                &sql,
                None,
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
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
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows_with_processing<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
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
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_query_rows_with_transformation<
        TIn: SqlWhereModel + Send + Sync + 'static,
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
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_db_entity<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.connection
            .insert_db_entity(
                entity,
                table_name,
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
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_db_entities<TEntity: SqlInsertModel>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .bulk_insert_db_entities(
                entities,
                table_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_db_entities_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        table_name: &str,
        entities: &[TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .bulk_insert_db_entities_if_not_exists(
                table_name,
                entities,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn update_db_entity<'s, TEntity: SqlUpdateModel + SqlWhereModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        self.connection
            .update_db_entity(
                entity,
                table_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_or_update_db_entity<'s, TEntity: SqlInsertModel + SqlUpdateModel>(
        &self,
        table_name: &str,
        update_conflict_type: UpdateConflictType<'s>,
        entities: &[TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .bulk_insert_or_update_db_entity(
                table_name,
                &update_conflict_type,
                entities,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_or_update_db_entity<'s, TEntity: SqlInsertModel + SqlUpdateModel>(
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
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn delete_db_entity<TWhereModel: SqlWhereModel>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .delete_db_entity(
                table_name,
                where_model,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_delete<TEntity: SqlWhereModel>(
        &self,
        table_name: &str,
        entities: &[TEntity],

        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        self.connection
            .bulk_delete(
                table_name,
                entities,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn concurrent_insert_or_update_single_entity<
        's,
        TModel: SelectEntity + SqlInsertModel + SqlUpdateModel + SqlWhereModel,
        TWhereModel: SqlWhereModel,
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
        SqlOperationWithRetries::new(self.connection.clone(), delay_between_retries, retries)
    }
}
