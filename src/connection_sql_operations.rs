use crate::{sql_insert::SqlInsertModel, MyPostgresError, PostgresConnection};

impl PostgresConnection {
    pub async fn insert_db_entity_if_not_exists<TEntity: SqlInsertModel>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<
            &my_telemetry::MyTelemetryContext,
        >,
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
}
