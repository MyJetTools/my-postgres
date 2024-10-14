use std::{collections::HashMap, time::Duration};

use my_logger::LogEventCtx;
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use crate::{
    table_schema::{IndexSchema, TableSchema, DEFAULT_SCHEMA},
    MyPostgresError, PostgresConnection, RequestContext,
};

pub async fn sync_indexes(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
) -> Result<bool, MyPostgresError> {
    if table_schema.indexes.is_none() {
        #[cfg(not(feature = "with-logs-and-telemetry"))]
        println!(
            "Table {} has no indexes. Skipping synchronization",
            table_schema.table_name
        );

        my_logger::LOGGER.write_info(
            "Table Schema verification",
            format!(
                "No Schema indexes is found for the table {}. Indexes synchronization is skipping",
                table_schema.table_name
            ),
            LogEventCtx::new().add("table_name", table_schema.table_name.to_string()),
        );

        return Ok(false);
    }

    let table_schema_indexes = table_schema.indexes.as_ref().unwrap();

    let indexes_from_db = get_indexes_from_db(
        conn_string,
        table_schema.table_name,
        sql_timeout,
        #[cfg(feature = "with-logs-and-telemetry")]
        ctx,
    )
    .await?;

    let mut has_updates = false;

    for (index_name, index_schema) in table_schema_indexes {
        if let Some(index_from_db) = indexes_from_db.get(index_name) {
            if !index_schema.is_the_same_with(index_from_db) {
                println!("Index {} is not synchronized", index_name);
                update_index(
                    conn_string,
                    table_schema,
                    index_name,
                    index_schema,
                    sql_timeout,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    ctx,
                )
                .await?;
            }
        } else {
            println!("Index {} not found. Creating one", index_name);
            create_index(
                conn_string,
                table_schema,
                index_name,
                index_schema,
                sql_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                ctx,
            )
            .await?;
            has_updates = true;
        }
    }

    Ok(has_updates)
}

async fn create_index(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    index_name: &str,
    index_schema: &IndexSchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
) -> Result<(), MyPostgresError> {
    let sql = index_schema.generate_create_index_sql(&table_schema.table_name, index_name);

    println!("Executing sql: {}", sql);

    my_logger::LOGGER.write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!("Executing sql: {}", sql),
        LogEventCtx::new()
            .add("table_name", table_schema.table_name.to_string())
            .add("index_name", index_name),
    );

    let ctx = RequestContext::new(
        sql_timeout,
        "create_new_index".to_string(),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(&ctx),
    );

    conn_string.execute_sql(&sql.into(), &ctx).await?;

    Ok(())
}

async fn update_index(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    index_name: &str,
    index_schema: &IndexSchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
) -> Result<(), MyPostgresError> {
    let sql = format!("drop index {index_name};");

    println!("Executing sql: {}", sql);

    my_logger::LOGGER.write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!("Executing sql: {}", sql),
        LogEventCtx::new()
            .add("table_name", table_schema.table_name.to_string())
            .add("index_name", index_name),
    );

    create_index(
        conn_string,
        table_schema,
        index_name,
        index_schema,
        sql_timeout,
        #[cfg(feature = "with-logs-and-telemetry")]
        ctx,
    )
    .await?;

    Ok(())
}

async fn get_indexes_from_db(
    conn_string: &PostgresConnection,
    table_name: &str,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
) -> Result<HashMap<String, IndexSchema>, MyPostgresError> {
    let schema = DEFAULT_SCHEMA;
    // cSpell: disable
    let sql = format!(
    "select indexname, indexdef from pg_indexes where schemaname = '{schema}' AND tablename = '{table_name}'"
);

    let ctx = RequestContext::new(
        sql_timeout,
        "get_indexes_from_db".to_string(),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(ctx),
    );

    let result = conn_string
        .execute_sql_as_vec(
            &sql.into(),
            |db_row| {
                let index_name: String = db_row.get("indexname");
                let index_def: String = db_row.get("indexdef");
                (index_name, index_def)
            },
            &ctx,
        )
        .await?;
    // cSpell: enable

    let mut as_has_map = HashMap::new();

    for (index_name, index_def) in result {
        as_has_map.insert(index_name, IndexSchema::from_index_def(&index_def));
    }

    Ok(as_has_map)
}
