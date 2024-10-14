use std::time::Duration;

use my_logger::LogEventCtx;
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use crate::{
    table_schema::{PrimaryKeySchema, TableSchema},
    ColumnName, MyPostgresError, PostgresConnection, RequestContext,
};

pub async fn sync_primary_key(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) -> Result<bool, MyPostgresError> {
    if table_schema.primary_key.is_none() {
        my_logger::LOGGER.write_info(
            "Table Schema verification",
            format!(
                "No Primary key is found for the table {}. Primary key synchronization is skipping",
                table_schema.table_name
            ),
            LogEventCtx::new().add("table_name", table_schema.table_name.to_string()),
        );

        return Ok(false);
    }

    let (primary_key_name, primary_key_schema) = table_schema.primary_key.as_ref().unwrap();

    let primary_key_from_db = get_primary_key_fields_from_db(
        conn_string,
        table_schema.table_name,
        sql_timeout,
        #[cfg(feature = "with-logs-and-telemetry")]
        my_telemetry,
    )
    .await?;

    if primary_key_schema.is_same_with(&primary_key_from_db) {
        return Ok(false);
    }

    update_primary_key(
        conn_string,
        &table_schema.table_name,
        primary_key_name,
        primary_key_schema,
        &primary_key_from_db,
        sql_timeout,
        #[cfg(feature = "with-logs-and-telemetry")]
        my_telemetry,
    )
    .await;

    Ok(true)
}

async fn update_primary_key(
    conn_string: &PostgresConnection,
    table_name: &str,
    primary_key_name: &str,
    primary_key_schema: &PrimaryKeySchema,
    primary_key_from_db: &PrimaryKeySchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) {
    let update_primary_key_sql = primary_key_schema.generate_update_primary_key_sql(
        table_name,
        primary_key_name,
        primary_key_from_db,
    );

    if update_primary_key_sql.is_none() {
        return;
    }

    let update_primary_key_sql = update_primary_key_sql.unwrap();

    my_logger::LOGGER.write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!(
            "Executing update primary key sql: {:?}",
            update_primary_key_sql
        ),
        LogEventCtx::new()
            .add("table_name", table_name.to_string())
            .add("primary_key", "primary_key_name"),
    );

    for sql in update_primary_key_sql {
        let ctx = RequestContext::new(
            sql_timeout,
            "update_primary_key".to_string(),
            #[cfg(feature = "with-logs-and-telemetry")]
            Some(my_telemetry),
        );
        conn_string.execute_sql(&sql.into(), &ctx).await.unwrap();
    }
}

async fn get_primary_key_fields_from_db(
    conn_string: &PostgresConnection,
    table_name: &str,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) -> Result<PrimaryKeySchema, MyPostgresError> {
    // cSpell: disable
    let sql = format!(
        r#"SELECT column_name
        FROM information_schema.key_column_usage
        WHERE constraint_name = (
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE table_name = '{table_name}'
          AND constraint_type = 'PRIMARY KEY'
        )
        AND table_name = '{table_name}'
        ORDER BY ordinal_position;"#
    );

    // cSpell: enable

    let ctx = RequestContext::new(
        sql_timeout,
        "get_primary_key_fields_from_db".to_string(),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(my_telemetry),
    );
    let result = conn_string
        .execute_sql_as_vec(
            &sql.into(),
            |db_row| {
                let result: String = db_row.get(0);
                ColumnName::new(result.into())
            },
            &ctx,
        )
        .await?;

    Ok(PrimaryKeySchema::from_vec(result))
}
