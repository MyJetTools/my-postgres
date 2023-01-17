#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{
    count_result::CountResult,
    sql_insert::SqlInsertModel,
    sql_select::{BulkSelectBuilder, BulkSelectEntity, SelectEntity, ToSqlString},
    sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel,
    MyPostgressError, PostgresConnection, PostgresConnectionInstance, PostgressSettings,
};

pub struct MyPostgres {
    connection: Arc<PostgresConnection>,
}

impl MyPostgres {
    pub async fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let connection = PostgresConnectionInstance::new(
            app_name,
            postgres_settings,
            Duration::from_secs(5),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        );

        Self {
            connection: Arc::new(PostgresConnection::Single(connection)),
        }
    }

    pub async fn with_shared_connection(connection: Arc<PostgresConnection>) -> Self {
        Self { connection }
    }

    pub async fn get_count<'s, TWhereModel: SqlWhereModel<'s>, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgressError> {
        let mut sql = String::new();

        let mut params = Vec::new();
        sql.push_str("SELECT COUNT(*)::");
        sql.push_str(TResult::get_postgres_type());

        sql.push_str(" FROM ");
        sql.push_str(table_name);
        sql.push_str(" WHERE ");

        where_model.fill_where(&mut sql, &mut params);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        let mut result = self
            .connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let (sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        let mut result = self
            .connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let (mut sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        post_processing(&mut sql);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        let mut result = self
            .connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<u64, MyPostgressError> {
        let (sql, params) = sql.as_sql();

        let params = if let Some(params) = params {
            params
        } else {
            &[]
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
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let (sql, params) = sql.as_sql();

        let params = if let Some(params) = params {
            params
        } else {
            &[]
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
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let (sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql_as_vec(
                sql.as_str(),
                params_to_invoke.as_slice(),
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
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let (mut sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        post_processing(&mut sql);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql_as_vec(
                sql.as_str(),
                params_to_invoke.as_slice(),
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
    ) -> Result<Vec<TOut>, MyPostgressError> {
        let process_name = format!("BulkQueryRows: {}", sql_builder.table_name);
        let (sql, params) = sql_builder.build_sql(|sql| TEntity::fill_select_fields(sql));

        let response = {
            let mut params_to_invoke = Vec::with_capacity(params.len());

            for param in &params {
                params_to_invoke.push(param.get_value());
            }

            self.connection
                .execute_sql_as_vec(
                    sql.as_str(),
                    &params_to_invoke,
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
    ) -> Result<(), MyPostgressError> {
        let mut params = Vec::new();
        let (sql, _) = crate::sql_insert::build_insert(table_name, entity, &mut params, None);

        let process_name: String = format!("insert_db_entity into table {}", table_name);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<(), MyPostgressError> {
        let mut params = Vec::new();
        let sql = crate::sql_insert::build_insert_if_not_exists(table_name, entity, &mut params);

        let process_name = format!("insert_db_entity_if_not_exists into table {}", table_name);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<(), MyPostgressError> {
        let (sql, params) = crate::sql_insert::build_bulk_insert(table_name, entities);

        let process_name = format!("bulk_insert_db_entities into table {}", table_name);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<(), MyPostgressError> {
        let (mut sql, params) = crate::sql_insert::build_bulk_insert(table_name, entities);

        sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!(
            "bulk_insert_db_entities_if_not_exists into table {}",
            table_name
        );

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<(), MyPostgressError> {
        let (sql, params) = crate::sql_update::build(table_name, entity, entity);
        let process_name = format!("update_db_entity into table {}", table_name);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
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
        pk_name: &str,
        entities: &'s [TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!(
            "bulk_insert_or_update_db_entity into table {} {} entities",
            table_name,
            entities.len()
        );

        let sqls = crate::sql_insert::build_bulk_insert_if_update(table_name, pk_name, entities);

        self.connection
            .execute_bulk_sql(
                sqls,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    >(
        &self,
        table_name: &str,
        pk_name: &str,
        entity: &'s TEntity,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("insert_or_update_db_entity into table {}", table_name);

        let (sql, params) = crate::sql_insert::build_insert_or_update(table_name, pk_name, entity);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
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
    ) -> Result<(), MyPostgressError> {
        let (sql, params) = crate::sql_delete::build_delete(table_name, where_model);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                params_to_invoke.as_slice(),
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
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("bulk_delete from table {}", table_name);

        let (sql, params) = crate::sql_delete::build_bulk_delete(table_name, entities);

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in &params {
            params_to_invoke.push(param.get_value());
        }

        self.connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                &process_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }
}
