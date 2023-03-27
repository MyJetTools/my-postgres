use std::collections::HashMap;

use crate::{MyPostgresError, PostgresConnection};

use super::{IndexSchema, PrimaryKeySchema, SchemaDifference, TableColumn, TableColumnType};
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

        if let Some((primary_key_name, primary_key_schema)) = &table_schema.primary_key {
            let primary_key_from_db =
                get_primary_key_fields_from_db(conn_string, table_schema.table_name).await?;

            if !primary_key_schema.is_same_with(&primary_key_from_db) {
                update_primary_key(
                    conn_string,
                    &table_schema.table_name,
                    primary_key_name,
                    primary_key_schema,
                    &primary_key_from_db,
                )
                .await;
            }
        }

        if let Some(table_schema_indexes) = &table_schema.indexes {
            let indexes_from_db = get_indexes_from_db(conn_string, table_schema.table_name).await?;

            let mut has_updates = false;

            for (index_name, index_schema) in table_schema_indexes {
                if let Some(index_from_db) = indexes_from_db.get(index_name) {
                    if !index_schema.is_the_same_with(index_from_db) {
                        println!("Index {} is the synchronized", index_name);
                        update_index(conn_string, table_schema, index_name, index_schema).await;
                        has_updates = true;
                    }
                } else {
                    println!("Index {} not found. Creating", index_name);
                    create_index(conn_string, table_schema, index_name, index_schema).await;
                    has_updates = true;
                }
            }

            if has_updates {
                continue;
            }
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        {
            logger.write_info(
                "Table Schema verification".into(),
                format!(
                    "Db Schema is up to date for a table, {}",
                    table_schema.table_name
                ),
                None,
            );
        }

        #[cfg(not(feature = "with-logs-and-telemetry"))]
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
    let result = conn_string
        .execute_sql_as_vec(&sql, &[], "get_db_fields", |db_row| TableColumn {
            name: db_row.get("column_name"),
            sql_type: get_sql_type(db_row),
            is_nullable: get_is_nullable(db_row),
            default: None,
        })
        .await?;

    #[cfg(feature = "with-logs-and-telemetry")]
    let result = conn_string
        .execute_sql_as_vec(
            &sql,
            &[],
            "get_db_fields",
            |db_row| TableColumn {
                name: db_row.get("column_name"),
                sql_type: get_sql_type(db_row),
                is_nullable: get_is_nullable(db_row),
                default: None,
            },
            None,
        )
        .await?;

    if result.is_empty() {
        return Ok(None);
    }

    Ok(Some(rust_extensions::linq::to_hash_map(
        result.into_iter(),
        |itm| (itm.name.clone(), itm),
    )))
}

async fn get_primary_key_fields_from_db(
    conn_string: &PostgresConnection,
    table_name: &str,
) -> Result<PrimaryKeySchema, MyPostgresError> {
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

    Ok(PrimaryKeySchema::from_vec_of_string(result))
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

    ctx.insert("TableName".to_string(), table_schema.table_name.to_string());

    if let Some((primary_key_name, _)) = &table_schema.primary_key {
        ctx.insert("primaryKeyName".to_string(), primary_key_name.to_string());
    }

    ctx.insert("Sql".to_string(), create_table_sql.to_string());

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
    table_name: &str,
    primary_key_name: &str,
    primary_key_schema: &PrimaryKeySchema,
    primary_key_from_db: &PrimaryKeySchema,
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

async fn get_indexes_from_db(
    conn_string: &PostgresConnection,
    table_name: &str,
) -> Result<HashMap<String, IndexSchema>, MyPostgresError> {
    let schema = DEFAULT_SCHEMA;
    // cSpell: disable
    let sql = format!(
        "select indexname, indexdef from pg_indexes where schemaname = '{schema}' AND tablename = '{table_name}'"
    );

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    let result = conn_string
        .execute_sql_as_vec(&sql, &[], "get_db_fields", |db_row| {
            let index_name: String = db_row.get("indexname");
            let index_def: String = db_row.get("indexdef");
            (index_name, index_def)
        })
        .await?;
    #[cfg(feature = "with-logs-and-telemetry")]
    let result = conn_string
        .execute_sql_as_vec(
            &sql,
            &[],
            "get_db_fields",
            |db_row| {
                let index_name: String = db_row.get("indexname");
                let index_def: String = db_row.get("indexdef");
                (index_name, index_def)
            },
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

async fn create_index(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    index_name: &str,
    index_schema: &IndexSchema,
) {
    let sql = index_schema.generate_create_index_sql(&table_schema.table_name, index_name);

    println!("Executing sql: {}", sql);

    #[cfg(feature = "with-logs-and-telemetry")]
    conn_string
        .execute_sql(&sql, &[], "create_new_index", None)
        .await
        .unwrap();

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    conn_string
        .execute_sql(&sql, &[], "create_new_index")
        .await
        .unwrap();
}

async fn update_index(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    index_name: &str,
    index_schema: &IndexSchema,
) {
    let sql = format!("drop index {index_name};");

    println!("Executing sql: {}", sql);

    #[cfg(feature = "with-logs-and-telemetry")]
    conn_string
        .execute_sql(&sql, &[], "create_new_index", None)
        .await
        .unwrap();

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    conn_string
        .execute_sql(&sql, &[], "create_new_index")
        .await
        .unwrap();

    create_index(conn_string, table_schema, index_name, index_schema).await;
}
