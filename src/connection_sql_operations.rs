use crate::{
    count_result::CountResult,
    sql::{SelectBuilder, SqlData, SqlValues},
    sql_insert::SqlInsertModel,
    sql_select::SelectEntity,
    sql_where::SqlWhereModel,
    MyPostgresError, PostgresConnection,
};
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

impl PostgresConnection {
    pub async fn insert_db_entity_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgresError> {
        let mut sql_data = crate::sql::build_insert_sql(entity, table_name);
        sql_data.sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!("insert_db_entity_if_not_exists into table {}", table_name);

        self.execute_sql(
            sql_data,
            process_name.as_str().into(),
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await
    }

    pub async fn get_count<TWhereModel: SqlWhereModel, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: &TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgresError> {
        let mut sql = String::new();

        let mut values = SqlValues::new();
        sql.push_str("SELECT COUNT(*)::");
        sql.push_str(TResult::get_postgres_type());

        sql.push_str(" FROM ");
        sql.push_str(table_name);

        let where_condition = where_model.build_where_sql_part(&mut values);

        if where_condition.has_conditions() {
            sql.push_str(" WHERE ");
            where_condition.build(&mut sql);
        }
        where_model.fill_limit_and_offset(&mut sql);

        let mut result = self
            .execute_sql_as_vec(
                SqlData::new(sql, values),
                None,
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
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let sql = select_builder.build_select_sql(table_name, where_model);

        let mut result = self
            .execute_sql_as_vec(
                sql,
                None,
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
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgresError> {
        let select_builder = SelectBuilder::from_select_model::<TEntity>();

        let mut sql = select_builder.build_select_sql(table_name, where_model);

        post_processing(&mut sql.sql);

        let mut result = self
            .execute_sql_as_vec(
                sql,
                None,
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
}
