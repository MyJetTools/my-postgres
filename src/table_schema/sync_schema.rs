use std::collections::HashMap;

use crate::{MyPostgresError, PostgresConnection};

use super::{SchemaDifference, TableColumn, TableColumnType};
use super::{TableSchema, DEFAULT_SCHEMA};

pub async fn sync_schema(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &std::sync::Arc<
        dyn rust_extensions::Logger + Sync + Send + 'static,
    >,
) -> Result<(), MyPostgresError> {
    loop {
        let db_fields = get_db_fields(conn_string, table_schema.table_name).await?;

        if db_fields.is_none() {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            create_table(conn_string, table_schema).await;
            #[cfg(feature = "with-logs-and-telemetry")]
            create_table(conn_string, table_schema, logger).await;
            return Ok(());
        }

        let db_fields = db_fields.as_ref().unwrap();

        let schema_difference = SchemaDifference::new(table_schema, db_fields);

        if schema_difference.to_update.len() > 0 {
            panic!(
                "Please update columns manually {:?}",
                schema_difference.to_update
            );
        }

        if schema_difference.to_add.len() > 0 {
            for column_name in &schema_difference.to_add {
                add_column_to_table(conn_string, table_schema, column_name).await;
            }

            continue;
        }

        if schema_difference.primary_key_is_different {
            update_primary_key(conn_string, table_schema, has_primary_key_in_db(db_fields)).await;
            continue;
        }

        println!(
            "Db Schema is up to date for a table: {}",
            table_schema.table_name
        );

        return Ok(());
    }
}

async fn get_db_fields(
    conn_string: &PostgresConnection,
    table_name: &str,
) -> Result<Option<HashMap<String, TableColumn>>, MyPostgresError> {
    let sql = format!(
        r#"SELECT column_name, column_default, is_nullable, data_type
    FROM information_schema.columns
   WHERE table_schema = '{DEFAULT_SCHEMA}'
     AND table_name   = '{table_name}'
    ORDER BY ordinal_position"#
    );

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    let mut result = conn_string
        .execute_sql_as_vec(&sql, &[], "get_db_fields", |db_row| TableColumn {
            name: db_row.get("column_name"),
            sql_type: get_sql_type(db_row),
            is_primary_key: None,
            is_nullable: get_is_nullable(db_row),
            default: None,
        })
        .await?;

    #[cfg(feature = "with-logs-and-telemetry")]
    let mut result = conn_string
        .execute_sql_as_vec(
            &sql,
            &[],
            "get_db_fields",
            |db_row| TableColumn {
                name: db_row.get("column_name"),
                sql_type: get_sql_type(db_row),
                is_primary_key: None,
                is_nullable: get_is_nullable(db_row),
                default: None,
            },
            None,
        )
        .await?;

    if result.is_empty() {
        return Ok(None);
    }

    if let Some(primary_keys) = get_primary_key_fields(conn_string, table_name).await? {
        let mut no = 0;
        for primary_key in primary_keys {
            if let Some(column) = result.iter_mut().find(|itm| itm.name == primary_key) {
                column.is_primary_key = Some(no);
            }
            no += 1;
        }
    }

    Ok(Some(rust_extensions::linq::to_hash_map(
        result.into_iter(),
        |itm| (itm.name.clone(), itm),
    )))
}

async fn get_primary_key_fields(
    conn_string: &PostgresConnection,
    table_name: &str,
) -> Result<Option<Vec<String>>, MyPostgresError> {
    // cSpell: disable
    let sql = format!(
        r#"SELECT a.attname AS name
        FROM
            pg_class AS c
            JOIN pg_index AS i ON c.oid = i.indrelid AND i.indisprimary
            JOIN pg_attribute AS a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey)
        WHERE c.oid = '{table_name}'::regclass"#
    );

    // cSpell: enable

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    let result = conn_string
        .execute_sql_as_vec(&sql, &[], "get_db_fields", |db_row| {
            let result: String = db_row.get(0);
            result
        })
        .await?;
    #[cfg(feature = "with-logs-and-telemetry")]
    let result = conn_string
        .execute_sql_as_vec(
            &sql,
            &[],
            "get_db_fields",
            |db_row| {
                let result: String = db_row.get(0);
                result
            },
            None,
        )
        .await?;

    if result.is_empty() {
        return Ok(None);
    }

    Ok(Some(result))
}

fn get_sql_type(db_row: &tokio_postgres::Row) -> TableColumnType {
    let column_type: String = db_row.get("data_type");
    match TableColumnType::from_db_string(&column_type) {
        Some(result) => result,
        None => {
            panic!("Unknown column type: {}", column_type);
        }
    }
}

fn get_is_nullable(db_row: &tokio_postgres::Row) -> bool {
    let is_nullable: String = db_row.get("is_nullable");
    is_nullable == "YES"
}

#[cfg(not(feature = "with-logs-and-telemetry"))]
async fn create_table(conn_string: &PostgresConnection, table_schema: &TableSchema) {
    println!("Table not found. Creating Table");
    let create_table_sql = table_schema.generate_create_table_script();
    conn_string
        .execute_sql(&create_table_sql, &[], "create_table")
        .await
        .unwrap();
}

#[cfg(feature = "with-logs-and-telemetry")]
async fn create_table(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &std::sync::Arc<
        dyn rust_extensions::Logger + Sync + Send + 'static,
    >,
) {
    let create_table_sql = table_schema.generate_create_table_script();

    let mut ctx = HashMap::new();

    ctx.insert(
        "table_name".to_string(),
        table_schema.table_name.to_string(),
    );

    if let Some(primary_key_name) = &table_schema.primary_key_name {
        ctx.insert("primary_key_name".to_string(), primary_key_name.to_string());
    }

    ctx.insert("sql".to_string(), create_table_sql.to_string());

    logger.write_warning(
        "check_schema".to_string(),
        format!("Creating table: {}", table_schema.table_name),
        Some(ctx),
    );
    conn_string
        .execute_sql(&create_table_sql, &[], "create_table", None)
        .await
        .unwrap();
}

async fn add_column_to_table(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    column_name: &str,
) {
    let add_column_sql = table_schema.generate_add_column_sql(column_name);

    println!(
        "Adding column by execution sql: {}",
        add_column_sql.as_str()
    );

    #[cfg(feature = "with-logs-and-telemetry")]
    conn_string
        .execute_sql(&add_column_sql, &[], "add_column_to_table", None)
        .await
        .unwrap();

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    conn_string
        .execute_sql(&add_column_sql, &[], "add_column_to_table")
        .await
        .unwrap();
}

async fn update_primary_key(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    has_primary_key_in_db: bool,
) {
    let update_primary_key_sql =
        table_schema.generate_update_primary_key_sql(has_primary_key_in_db);

    for sql in update_primary_key_sql {
        println!("Executing update primary key sql: {}", sql);

        #[cfg(feature = "with-logs-and-telemetry")]
        conn_string
            .execute_sql(&sql, &[], "update_primary_key", None)
            .await
            .unwrap();

        #[cfg(not(feature = "with-logs-and-telemetry"))]
        conn_string
            .execute_sql(&sql, &[], "update_primary_key")
            .await
            .unwrap();
    }
}

fn has_primary_key_in_db(fields: &HashMap<String, TableColumn>) -> bool {
    for (_, field) in fields {
        if field.is_primary_key.is_some() {
            return true;
        }
    }
    false
}
