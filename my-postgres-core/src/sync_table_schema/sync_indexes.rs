use std::{collections::HashMap, time::Duration};

use crate::{
    table_schema::{IndexSchema, TableSchema, DEFAULT_SCHEMA},
    MyPostgresError, PostgresConnection,
};

pub async fn sync_indexes(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    sql_timeout: Duration,
) -> Result<bool, MyPostgresError> {
    if table_schema.indexes.is_none() {
        #[cfg(not(feature = "with-logs-and-telemetry"))]
        println!(
            "Table {} has no indexes. Skipping synchronization",
            table_schema.table_name
        );

        #[cfg(feature = "with-logs-and-telemetry")]
        conn_string.get_logger().write_info(
            "Table Schema verification".into(),
            format!(
                "No Schema indexes is found for the table {}. Indexes synchronization is skipping",
                table_schema.table_name
            ),
            None,
        );

        return Ok(false);
    }

    let table_schema_indexes = table_schema.indexes.as_ref().unwrap();

    let indexes_from_db =
        get_indexes_from_db(conn_string, table_schema.table_name, sql_timeout).await?;

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
) -> Result<(), MyPostgresError> {
    let sql = index_schema.generate_create_index_sql(&table_schema.table_name, index_name);

    println!("Executing sql: {}", sql);

    #[cfg(feature = "with-logs-and-telemetry")]
    conn_string.get_logger().write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!("Executing sql: {}", sql),
        None,
    );

    conn_string
        .execute_sql(
            &sql.into(),
            "create_new_index".into(),
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await?;

    Ok(())
}

async fn update_index(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    index_name: &str,
    index_schema: &IndexSchema,
    sql_timeout: Duration,
) -> Result<(), MyPostgresError> {
    let sql = format!("drop index {index_name};");

    println!("Executing sql: {}", sql);

    #[cfg(feature = "with-logs-and-telemetry")]
    conn_string.get_logger().write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!("Executing sql: {}", sql),
        None,
    );

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    conn_string
        .execute_sql(
            &sql.into(),
            "create_new_index".into(),
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await
        .unwrap();

    create_index(
        conn_string,
        table_schema,
        index_name,
        index_schema,
        sql_timeout,
    )
    .await?;

    Ok(())
}

async fn get_indexes_from_db(
    conn_string: &PostgresConnection,
    table_name: &str,
    sql_timeout: Duration,
) -> Result<HashMap<String, IndexSchema>, MyPostgresError> {
    let schema = DEFAULT_SCHEMA;
    // cSpell: disable
    let sql = format!(
    "select indexname, indexdef from pg_indexes where schemaname = '{schema}' AND tablename = '{table_name}'"
);

    let result = conn_string
        .execute_sql_as_vec(
            &sql.into(),
            "get_db_fields".into(),
            sql_timeout,
            |db_row| {
                let index_name: String = db_row.get("indexname");
                let index_def: String = db_row.get("indexdef");
                (index_name, index_def)
            },
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await?;
    // cSpell: enable

    let mut as_has_map = HashMap::new();

    for (index_name, index_def) in result {
        as_has_map.insert(index_name, IndexSchema::from_index_def(&index_def));
    }

    Ok(as_has_map)
}
