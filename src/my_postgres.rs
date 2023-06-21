#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use rust_extensions::date_time::DateTimeAsMicroseconds;
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{
    count_result::CountResult,
    sql::{SelectBuilder, SqlValues},
    sql_insert::SqlInsertModel,
    sql_select::{BulkSelectBuilder, BulkSelectEntity, SelectEntity, ToSqlString},
    sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel,
    table_schema::{PrimaryKeySchema, TableSchema, TableSchemaProvider},
    MyPostgresError, PostgresConnection, PostgresConnectionInstance, PostgresSettings,
    UpdateConflictType,
};

pub struct MyPostgres {
    connection: Arc<PostgresConnection>,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Sync + Send + 'static>,
}

impl MyPostgres {
    pub fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
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

    pub async fn get_count<'s, TWhereModel: SqlWhereModel<'s>, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgresError> {
        let mut sql = String::new();

        let mut params = SqlValues::new();
        sql.push_str("SELECT COUNT(*)::");
        sql.push_str(TResult::get_postgres_type());

        sql.push_str(" FROM ");
        sql.push_str(table_name);

        let where_condition = where_model.build_where_sql_part(&mut params);

        if where_condition.has_conditions() {
            sql.push_str(" WHERE ");
            where_condition.build(&mut sql);
        }
        where_model.fill_limit_and_offset(&mut sql);

        let mut result = self
            .connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params,
                sql.as_str(),
                |row| TResult::from_db_row(row),
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

    pub async fn query_single_row<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
    >(
        &self,
        table_name: &str,
        where_model: Option<&'s TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let (sql, params) = select_builder.build_select_sql(table_name, where_model);

        let mut result = self
            .connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params,
                sql.as_str(),
                |row| TEntity::from(row),
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

    pub async fn query_single_row_with_processing<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&'s TWhereModel>,
        post_processing: TPostProcessing,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let (mut sql, params) = select_builder.build_select_sql(table_name, where_model);

        post_processing(&mut sql);

        let mut result = self
            .connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params,
                sql.as_str(),
                |row| TEntity::from(row),
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

    pub async fn execute_sql<ToSql: ToSqlString>(
        &self,
        sql: &ToSql,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let (sql, params) = sql.as_sql();

        let params = if let Some(params) = params {
            params
        } else {
            SqlValues::empty()
        };

        self.connection
            .execute_sql(
                sql.as_str(),
                params,
                sql.as_str(),
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
        sql: &ToSql,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let (sql, params) = sql.as_sql();

        let params = if let Some(params) = params {
            params
        } else {
            SqlValues::empty()
        };

        self.connection
            .execute_sql_as_vec(
                sql.as_str(),
                params,
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
    >(
        &self,
        table_name: &str,
        where_model: Option<&'s TWhereModel>,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let (sql, params) = select_builder.build_select_sql(table_name, where_model);

        self.connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params,
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows_with_processing<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&'s TWhereModel>,
        post_processing: TPostProcessing,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let (mut sql, params) = select_builder.build_select_sql(table_name, where_model);

        post_processing(&mut sql);

        self.connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params,
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_query_rows_with_transformation<
        's,
        TIn: SqlWhereModel<'s> + Send + Sync + 'static,
        TOut,
        TEntity: SelectEntity + BulkSelectEntity + Send + Sync + 'static,
        TTransform: Fn(&TIn, Option<TEntity>) -> TOut,
    >(
        &self,
        sql_builder: &'s BulkSelectBuilder<'s, TIn>,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] ctx: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TOut>, MyPostgresError> {
        let process_name = format!("BulkQueryRows: {}", sql_builder.table_name);
        let (sql, params) = sql_builder.build_sql::<TEntity>();

        let response = {
            self.connection
                .execute_sql_as_vec(
                    sql.as_str(),
                    &params,
                    process_name.as_str(),
                    |row| TEntity::from(row),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    ctx,
                )
                .await?
        };

        let mut result = Vec::with_capacity(response.len());

        let mut response_as_hashmap = BTreeMap::new();

        for entity in response {
            response_as_hashmap.insert(entity.get_line_no(), entity);
        }

        let mut line_no = 0;
        for where_model in &sql_builder.where_models {
            let out = response_as_hashmap.remove(&line_no);
            let item = transform(where_model, out);
            result.push(item);
            line_no += 1;
        }

        Ok(result)
    }

    pub async fn insert_db_entity<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let (sql, params) = entity.build_insert_sql(table_name);

        let process_name: String = format!("insert_db_entity into table {}", table_name);

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn insert_db_entity_if_not_exists<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let (mut sql, params) = entity.build_insert_sql(table_name);
        sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!("insert_db_entity_if_not_exists into table {}", table_name);

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_insert_db_entities<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entities: &'s [TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let (sql, params) = TEntity::build_bulk_insert_sql(table_name, entities);

        let process_name = format!("bulk_insert_db_entities into table {}", table_name);

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_insert_db_entities_if_not_exists<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        table_name: &str,
        entities: &'s [TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let (mut sql, params) = TEntity::build_bulk_insert_sql(table_name, entities);

        sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!(
            "bulk_insert_db_entities_if_not_exists into table {}",
            table_name
        );

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn update_db_entity<'s, TEntity: SqlUpdateModel<'s> + SqlWhereModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let (sql, params) = entity.build_update_sql(table_name, Some(entity));
        let process_name = format!("update_db_entity into table {}", table_name);

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    >(
        &self,
        table_name: &str,
        update_conflict_type: UpdateConflictType<'s>,
        entities: &'s [TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let process_name = format!(
            "bulk_insert_or_update_db_entity into table {} {} entities",
            table_name,
            entities.len()
        );

        let (sql, params) =
            TEntity::build_bulk_insert_or_update_sql(table_name, &update_conflict_type, entities);

        self.connection
            .execute_sql(
                &sql,
                &params,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    >(
        &self,
        table_name: &str,
        update_conflict_type: UpdateConflictType<'s>,
        entity: &'s TEntity,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let process_name = format!("insert_or_update_db_entity into table {}", table_name);

        let (sql, params) =
            TEntity::build_insert_or_update_sql(table_name, &update_conflict_type, entity);

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn delete_db_entity<'s, TWhereModel: SqlWhereModel<'s>>(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let (sql, params) = where_model.build_delete_sql(table_name);

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                sql.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_delete<'s, TEntity: SqlWhereModel<'s>>(
        &self,
        table_name: &str,
        entities: &'s [TEntity],

        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let process_name = format!("bulk_delete from table {}", table_name);

        let (sql, params) = TEntity::build_bulk_delete_sql(entities, table_name);

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                &process_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn concurrent_insert_or_update<
        's,
        TModel: SqlInsertModel<'s> + SqlUpdateModel<'s>,
        TWhereModel: SqlWhereModel<'s>,
    >(
        &self,
        table_name: &str,
        update_conflict_type: UpdateConflictType<'s>,
        insert_model: &'s TModel,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TModel>, MyPostgresError> {
        let process_name = "concurrent_insert_or_update";
        let (sql, params) = crate::sql_concurrent_ops::build_concurrent_insert_or_update(
            table_name,
            insert_model,
            where_model,
            update_conflict_type,
        );

        self.connection
            .execute_sql(
                sql.as_str(),
                &params,
                &process_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        todo!("Implement")
    }
}
