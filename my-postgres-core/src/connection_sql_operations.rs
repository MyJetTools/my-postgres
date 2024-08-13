use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use crate::{
    count_result::CountResult,
    sql::{SelectBuilder, SqlData, SqlValues, UsedColumns},
    sql_insert::SqlInsertModel,
    sql_select::{BulkSelectBuilder, BulkSelectEntity, SelectEntity},
    sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel,
    union::UnionModel,
    ConcurrentOperationResult, MyPostgresError, PostgresConnection, PostgresReadStream,
    UpdateConflictType,
};
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

impl PostgresConnection {
    pub async fn insert_db_entity_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let mut sql_data =
            crate::sql::build_insert_sql(entity, table_name, &mut UsedColumns::as_none());
        sql_data.sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!("insert_db_entity_if_not_exists into table {}", table_name);

        self.execute_sql(
            &sql_data,
            process_name.as_str().into(),
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await
    }

    pub async fn get_count<TWhereModel: SqlWhereModel, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgresError> {
        let mut sql = String::new();

        let mut values = SqlValues::new();
        sql.push_str("SELECT COUNT(*)::");
        sql.push_str(TResult::get_postgres_type());

        sql.push_str(" FROM ");
        sql.push_str(table_name);

        if let Some(where_model) = where_model {
            if where_model.has_conditions() {
                sql.push_str(" WHERE ");
                where_model.fill_where_component(&mut sql, &mut values);
            }

            where_model.fill_limit_and_offset(&mut sql);
        }

        let mut result = self
            .execute_sql_as_vec(
                &SqlData::new(sql, values),
                format!("SELECT COUNT(*) FROM {}...", table_name),
                sql_request_timeout,
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

    pub async fn query_single_row<TEntity: SelectEntity, TWhereModel: SqlWhereModel>(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let sql = select_builder.to_sql_string(table_name, where_model);

        let mut result = self
            .execute_sql_as_vec(
                &sql,
                format!("Select single row from {}", table_name),
                sql_request_timeout,
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
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        post_processing: &TPostProcessing,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let mut sql = select_builder.to_sql_string(table_name, where_model);

        post_processing(&mut sql.sql);

        let mut result = self
            .execute_sql_as_vec(
                &sql,
                format!("Select single row from {}", table_name),
                sql_request_timeout,
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

    pub async fn query_rows<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let sql = select_builder.to_sql_string(table_name, where_model);

        self.execute_sql_as_vec(
            &sql,
            format!("Select rows from {}", table_name),
            sql_request_timeout,
            |row| TEntity::from(row),
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await
    }

    pub async fn query_rows_as_stream<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<PostgresReadStream<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let sql = select_builder.to_sql_string(table_name, where_model);

        self.execute_sql_as_stream(
            &sql,
            format!("Select rows from {}", table_name),
            sql_request_timeout,
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
        post_processing: &TPostProcessing,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let mut sql = select_builder.to_sql_string(table_name, where_model);

        post_processing(&mut sql.sql);

        self.execute_sql_as_vec(
            &sql,
            format!("Select rows from {}", table_name),
            sql_request_timeout,
            |row| TEntity::from(row),
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
        transform: &TTransform,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] ctx: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TOut>, MyPostgresError> {
        let process_name = format!("BulkQueryRows: {}", sql_builder.table_name);
        let sql = sql_builder.build_sql::<TEntity>();

        let response = {
            self.execute_sql_as_vec(
                &sql,
                process_name,
                sql_request_timeout,
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

    pub async fn bulk_insert_db_entities<TEntity: SqlInsertModel>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        if entities.len() == 0 {
            return Ok(());
        }

        let used_columns = entities[0].get_insert_columns_list();

        let sql_data = crate::sql::build_bulk_insert_sql(entities, table_name, &used_columns);

        let process_name = format!("bulk_insert_db_entities into table {}", table_name);

        self.execute_sql(
            &sql_data,
            process_name,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn bulk_insert_db_entities_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        table_name: &str,
        entities: &[TEntity],
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        if entities.len() == 0 {
            return Ok(());
        }

        let used_columns = entities[0].get_insert_columns_list();

        let mut sql_data = crate::sql::build_bulk_insert_sql(entities, table_name, &used_columns);

        sql_data.sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!(
            "bulk_insert_db_entities_if_not_exists into table {}",
            table_name
        );

        self.execute_sql(
            &sql_data,
            process_name,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn bulk_insert_or_update_db_entity<'s, TEntity: SqlInsertModel + SqlUpdateModel>(
        &self,
        table_name: &str,
        update_conflict_type: &UpdateConflictType<'s>,
        entities: &[TEntity],
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        {
            let mut has_entities = HashSet::new();

            for entity in entities {
                let key = entity.get_primary_key_as_single_string();

                if has_entities.contains(&key) {
                    panic!("Duplicated entity in bulk_insert_or_update_db_entity for table: {}. PrimaryKey: {}", table_name, key);
                }

                has_entities.insert(key);
            }
        }

        let process_name = format!(
            "bulk_insert_or_update_db_entity into table {} {} entities",
            table_name,
            entities.len()
        );

        let sql_data = crate::sql::build_bulk_insert_or_update_sql(
            table_name,
            &update_conflict_type,
            entities,
        );

        self.execute_sql(
            &sql_data,
            process_name,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn insert_or_update_db_entity<'s, TEntity: SqlInsertModel + SqlUpdateModel>(
        &self,
        table_name: &str,
        update_conflict_type: &UpdateConflictType<'s>,
        entity: &TEntity,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let process_name = format!("insert_or_update_db_entity into table {}", table_name);

        let sql_data =
            crate::sql::build_insert_or_update_sql(entity, table_name, &update_conflict_type);

        self.execute_sql(
            &sql_data,
            process_name,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn delete_db_entity<TWhereModel: SqlWhereModel>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let sql_data = where_model.build_delete_sql(table_name);

        self.execute_sql(
            &sql_data,
            format!("Delete entity from {}", table_name),
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn bulk_delete<TEntity: SqlWhereModel>(
        &self,
        table_name: &str,
        entities: &[TEntity],
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let process_name = format!("bulk_delete from table {}", table_name);

        let sql_data = TEntity::build_bulk_delete_sql(entities, table_name);

        self.execute_sql(
            &sql_data,
            process_name,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn update_db_entity<'s, TEntity: SqlUpdateModel + SqlWhereModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let sql_data = crate::sql::build_update_sql(entity, table_name);
        let process_name = format!("update_db_entity into table {}", table_name);

        self.execute_sql(
            &sql_data,
            process_name,
            sql_request_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await
    }

    pub async fn bulk_query<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
    >(
        &self,
        table_name: &str,
        where_models: Vec<TWhereModel>,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<UnionModel<TEntity, TWhereModel>>, MyPostgresError> {
        let mut sql = String::new();
        let mut values = SqlValues::new();

        crate::union::compile_union_select::<TEntity, TWhereModel>(
            &mut sql,
            &mut values,
            table_name,
            &where_models,
        );

        let sql_data = SqlData::new(sql, values);

        let mut result_stream = self
            .execute_sql_as_row_stream(
                &sql_data,
                "Bulk query with union".to_string(),
                sql_request_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        let mut result: Vec<UnionModel<TEntity, TWhereModel>> = Vec::new();
        for where_model in where_models {
            result.push(UnionModel {
                where_model,
                items: Vec::new(),
            });
        }

        while let Some(db_row) = result_stream.get_next().await? {
            let line_no: i32 = db_row.get(0);
            let entity = TEntity::from(&db_row);
            result[line_no as usize].items.push(entity);
        }

        Ok(result)
    }

    pub async fn bulk_query_with_transformation<
        TEntity: SelectEntity + Send + Sync + 'static,
        TOut: Send + Sync + 'static,
        TWhereModel: SqlWhereModel,
    >(
        &self,
        table_name: &str,
        where_models: Vec<TWhereModel>,
        transformation: &impl Fn(TEntity) -> TOut,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<UnionModel<TOut, TWhereModel>>, MyPostgresError> {
        let mut sql = String::new();
        let mut values = SqlValues::new();

        crate::union::compile_union_select::<TEntity, TWhereModel>(
            &mut sql,
            &mut values,
            table_name,
            &where_models,
        );

        let sql_data = SqlData::new(sql, values);

        let mut result_stream = self
            .execute_sql_as_row_stream(
                &sql_data,
                "Bulk query with union".to_string(),
                sql_request_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        let mut result: Vec<UnionModel<TOut, TWhereModel>> = Vec::new();
        for where_model in where_models {
            result.push(UnionModel {
                where_model,
                items: Vec::new(),
            });
        }

        while let Some(db_row) = result_stream.get_next().await? {
            let line_no: i32 = db_row.get(0);
            let entity = TEntity::from(&db_row);
            result[line_no as usize].items.push(transformation(entity));
        }

        Ok(result)
    }

    pub async fn insert_db_entity<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let sql = crate::sql::build_insert_sql(entity, table_name, &mut UsedColumns::as_none());

        let process_name: String = format!("insert_db_entity into table {}", table_name);

        self.execute_sql(
            &sql,
            process_name.as_str().into(),
            sql_request_timeout,
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
        crate_new_model: &impl Fn() -> Option<TModel>,
        update_model: &impl Fn(&mut TModel) -> bool,
        sql_request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<ConcurrentOperationResult<TModel>, MyPostgresError> {
        loop {
            let mut found = self
                .query_single_row::<TModel, TWhereModel>(
                    table_name,
                    Some(where_model),
                    sql_request_timeout,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                )
                .await?;

            match &mut found {
                Some(found_model) => match update_model(found_model) {
                    true => {
                        let result = self
                            .update_db_entity(
                                found_model,
                                table_name,
                                sql_request_timeout,
                                #[cfg(feature = "with-logs-and-telemetry")]
                                telemetry_context,
                            )
                            .await?;

                        if result > 0 {
                            return Ok(ConcurrentOperationResult::Updated(found.unwrap()));
                        }
                    }
                    false => {
                        return Ok(ConcurrentOperationResult::UpdateCanceledOnModel(
                            found.unwrap(),
                        ))
                    }
                },
                None => {
                    let new_model = crate_new_model();

                    match &new_model {
                        Some(new_model_to_save) => {
                            let result = self
                                .insert_db_entity_if_not_exists(
                                    new_model_to_save,
                                    table_name,
                                    sql_request_timeout,
                                    #[cfg(feature = "with-logs-and-telemetry")]
                                    telemetry_context,
                                )
                                .await?;

                            if result > 0 {
                                return Ok(ConcurrentOperationResult::Created(new_model.unwrap()));
                            }
                        }
                        None => {
                            return Ok(ConcurrentOperationResult::CreatedCanceled);
                        }
                    }
                }
            }
        }
    }
}
