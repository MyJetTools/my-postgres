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
    RequestContext, UpdateConflictType,
};
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

impl PostgresConnection {
    pub async fn insert_db_entity_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let mut sql_data =
            crate::sql::build_insert_sql(entity, table_name, &mut UsedColumns::as_none());
        sql_data.sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!("insert_db_entity_if_not_exists into table {}", table_name);

        let ctx = RequestContext::new(
            sql_request_timeout,
            process_name,
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        self.execute_sql(&sql_data, &ctx).await
    }

    pub async fn get_count<TWhereModel: SqlWhereModel + std::fmt::Debug, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        is_debug: bool,
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

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("SELECT COUNT(*) FROM {}...", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self
            .execute_sql_as_vec(
                &SqlData::new(sql, values),
                |row| TResult::from_db_row(row),
                &ctx,
            )
            .await;

        match result {
            Ok(mut result) => {
                if result.len() > 0 {
                    Ok(Some(result.remove(0)))
                } else {
                    Ok(None)
                }
            }
            Err(err) => {
                if is_debug {
                    println!("Error getting count with Where model: {:?}", where_model);
                }
                Err(err)
            }
        }
    }

    pub async fn query_single_row<
        TEntity: SelectEntity,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let sql = select_builder.to_sql_string(table_name, where_model);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("query_single_row from {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self
            .execute_sql_as_vec(&sql, |row| TEntity::from(row), &ctx)
            .await;

        match result {
            Ok(mut result) => {
                if result.len() > 0 {
                    Ok(Some(result.remove(0)))
                } else {
                    Ok(None)
                }
            }
            Err(err) => {
                if is_debug {
                    println!(
                        "Error getting single row with Where model: {:?}",
                        where_model
                    );
                }
                Err(err)
            }
        }
    }

    pub async fn query_single_row_with_processing<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        post_processing: &TPostProcessing,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let mut sql = select_builder.to_sql_string(table_name, where_model);

        post_processing(&mut sql.sql);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("query_single_row_with_processing from {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self
            .execute_sql_as_vec(&sql, |row| TEntity::from(row), &ctx)
            .await;

        match result {
            Ok(mut result) => {
                return Ok(result.pop());
            }
            Err(err) => {
                if is_debug {
                    println!(
                        "Error getting single row with Where model: {:?}",
                        where_model
                    );
                }
                return Err(err);
            }
        }
    }

    pub async fn query_rows<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let sql = select_builder.to_sql_string(table_name, where_model);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("query_rows from {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self
            .execute_sql_as_vec(&sql, |row| TEntity::from(row), &ctx)
            .await;

        if result.is_err() {
            if is_debug {
                println!("Error getting rows with Where model: {:?}", where_model);
            }
        }

        result
    }

    pub async fn query_rows_as_stream<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<PostgresReadStream<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let sql = select_builder.to_sql_string(table_name, where_model);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("query_rows_as_stream from {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql_as_stream(&sql, ctx).await;

        if result.is_err() {
            if is_debug {
                println!(
                    "Error getting rows as stream with Where model: {:?}",
                    where_model
                );
            }
        }

        result
    }

    pub async fn query_rows_with_processing<
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: Option<&TWhereModel>,
        post_processing: &TPostProcessing,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let mut sql = select_builder.to_sql_string(table_name, where_model);

        post_processing(&mut sql.sql);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("query_rows_with_processing from {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self
            .execute_sql_as_vec(&sql, |row| TEntity::from(row), &ctx)
            .await;

        if result.is_err() {
            if is_debug {
                println!("Error getting rows with Where model: {:?}", where_model);
            }
        }

        result
    }

    pub async fn bulk_query_rows_with_transformation<
        TIn: SqlWhereModel + Send + Sync + 'static + std::fmt::Debug,
        TOut,
        TEntity: SelectEntity + BulkSelectEntity + Send + Sync + 'static,
        TTransform: Fn(&TIn, Option<TEntity>) -> TOut,
    >(
        &self,
        sql_builder: &BulkSelectBuilder<TIn>,
        transform: &TTransform,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] ctx: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TOut>, MyPostgresError> {
        let sql = sql_builder.build_sql::<TEntity>();

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("BulkQueryRows: {}", sql_builder.table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            ctx,
        );

        let response = self
            .execute_sql_as_vec(&sql, |row| TEntity::from(row), &ctx)
            .await;

        let response = match response {
            Ok(result) => result,
            Err(err) => {
                if is_debug {
                    println!(
                        "Error getting rows with Where model: {:?}",
                        sql_builder.where_models
                    );
                }
                return Err(err);
            }
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

    pub async fn bulk_insert_db_entities<TEntity: SqlInsertModel + std::fmt::Debug>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        if entities.len() == 0 {
            return Ok(());
        }

        let used_columns = entities[0].get_insert_columns_list();

        let sql_data = crate::sql::build_bulk_insert_sql(entities, table_name, &used_columns);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("bulk_insert_db_entities into table {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql_data, &ctx).await;

        if let Err(err) = result {
            if is_debug {
                println!("Error inserting entities: {:?}. Err: {:?}", entities, err);
            }

            return Err(err);
        }

        Ok(())
    }

    pub async fn bulk_insert_db_entities_if_not_exists<
        TEntity: SqlInsertModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        entities: &[TEntity],
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        if entities.len() == 0 {
            return Ok(());
        }

        let used_columns = entities[0].get_insert_columns_list();

        let mut sql_data = crate::sql::build_bulk_insert_sql(entities, table_name, &used_columns);

        sql_data.sql.push_str(" ON CONFLICT DO NOTHING");

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!(
                "bulk_insert_db_entities_if_not_exists into table {}",
                table_name
            ),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql_data, &ctx).await;

        if let Err(err) = result {
            if is_debug {
                println!(
                    "Error inserting if not exists entities: {:?}. Err: {:?}",
                    entities, err
                );
            }

            return Err(err);
        }

        Ok(())
    }

    pub async fn bulk_insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel + SqlUpdateModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        update_conflict_type: &UpdateConflictType<'s>,
        entities: &[TEntity],
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        {
            let mut has_entities = HashSet::new();

            for entity in entities {
                let key = entity.get_primary_key_as_single_string();

                if has_entities.contains(&key) {
                    return Err(MyPostgresError::Other(format!("Duplicated entity in bulk_insert_or_update_db_entity for table: {}. PrimaryKey: {}", table_name, key)));
                }

                has_entities.insert(key);
            }
        }

        let sql_data = crate::sql::build_bulk_insert_or_update_sql(
            table_name,
            &update_conflict_type,
            entities,
        );

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("bulk_insert_or_update_db_entity into table {}", table_name,),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql_data, &ctx).await;

        if let Err(err) = result {
            if is_debug {
                println!(
                    "Error inserting or updating entities: {:?}. Err: {:?}",
                    entities, err
                );
            }

            return Err(err);
        }

        Ok(())
    }

    pub async fn insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel + SqlUpdateModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        update_conflict_type: &UpdateConflictType<'s>,
        entity: &TEntity,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let sql_data =
            crate::sql::build_insert_or_update_sql(entity, table_name, &update_conflict_type);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("insert_or_update_db_entity into table {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql_data, &ctx).await;

        if let Err(err) = result {
            if is_debug {
                println!(
                    "Error inserting or updating entity: {:?}. Err: {:?}",
                    entity, err
                );
            }

            return Err(err);
        }

        Ok(())
    }

    pub async fn delete<TWhereModel: SqlWhereModel + std::fmt::Debug>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let sql_data = where_model.build_delete_sql(table_name);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("Delete from {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql_data, &ctx).await;

        if let Err(err) = result {
            if is_debug {
                println!("Error deleting entity: {:?}. Err: {:?}", where_model, err);
            }

            return Err(err);
        }

        Ok(())
    }

    pub async fn bulk_delete<TEntity: SqlWhereModel + std::fmt::Debug>(
        &self,
        table_name: &str,
        entities: &[TEntity],
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgresError> {
        let sql_data = TEntity::build_bulk_delete_sql(entities, table_name);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("bulk_delete from table {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql_data, &ctx).await;

        if let Err(err) = result {
            if is_debug {
                println!(
                    "Error bulk deleting entities: {:?}. Err: {:?}",
                    entities, err
                );
            }

            return Err(err);
        }

        Ok(())
    }

    pub async fn update_db_entity<'s, TEntity: SqlUpdateModel + SqlWhereModel + std::fmt::Debug>(
        &self,
        entity: &TEntity,
        table_name: &str,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let sql_data = crate::sql::build_update_sql(entity, table_name);

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("update_db_entity into table {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql_data, &ctx).await;

        if result.is_err() {
            if is_debug {
                println!("Error updating entity: {:?}", entity);
            }
        }

        result
    }

    pub async fn bulk_query<TEntity: SelectEntity, TWhereModel: SqlWhereModel + std::fmt::Debug>(
        &self,
        table_name: &str,
        where_models: Vec<TWhereModel>,
        sql_request_timeout: Duration,
        is_debug: bool,
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

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("Bulk query with union {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result_stream = self.execute_sql_as_row_stream(&sql_data, &ctx).await;

        if result_stream.is_err() {
            if is_debug {
                println!("Error executing Bulk query: {:?}", sql_data.sql);
            }
        }

        let mut result_stream = result_stream?;

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
        TEntity: SelectEntity,
        TOut,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_models: Vec<TWhereModel>,
        transformation: &impl Fn(TEntity) -> TOut,
        sql_request_timeout: Duration,
        is_debug: bool,
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

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("bulk_query_with_transformation {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let mut result_stream = self.execute_sql_as_row_stream(&sql_data, &ctx).await?;

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

    pub async fn insert_db_entity<TEntity: SqlInsertModel + std::fmt::Debug>(
        &self,
        entity: &TEntity,
        table_name: &str,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let sql = crate::sql::build_insert_sql(entity, table_name, &mut UsedColumns::as_none());

        let ctx = RequestContext::new(
            sql_request_timeout,
            format!("insert_db_entity into table {}", table_name),
            is_debug,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        );

        let result = self.execute_sql(&sql, &ctx).await;

        if result.is_err() {
            if is_debug {
                println!("Error inserting entity: {:?}. Err: {:?}", entity, result);
            }
        }

        result
    }

    pub async fn concurrent_insert_or_update_single_entity<
        's,
        TModel: SelectEntity + SqlInsertModel + SqlUpdateModel + SqlWhereModel + std::fmt::Debug,
        TWhereModel: SqlWhereModel + std::fmt::Debug,
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        crate_new_model: &impl Fn() -> Option<TModel>,
        update_model: &impl Fn(&mut TModel) -> bool,
        sql_request_timeout: Duration,
        is_debug: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<ConcurrentOperationResult<TModel>, MyPostgresError> {
        loop {
            let mut found = self
                .query_single_row::<TModel, TWhereModel>(
                    table_name,
                    Some(where_model),
                    sql_request_timeout,
                    is_debug,
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
                                is_debug,
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
                                    is_debug,
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
