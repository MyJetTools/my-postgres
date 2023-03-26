use std::collections::HashMap;

use crate::{MyPostgresError, PostgresConnection};

use super::{TableColumn, TableColumnType};
use super::{TableSchema, DEFAULT_SCHEMA};

pub async fn check_schema(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
) -> Result<(), MyPostgresError> {
    let db_fields = get_db_fields(conn_string, table_schema.table_name).await?;

    if db_fields.is_none() {
        println!("Please execute script to create Table");
        let create_table_sql = table_schema.generate_create_table_script();
        println!("{}", create_table_sql);

        panic!(
            "Stopped because - table {} does not exist",
            table_schema.table_name
        );
    }

    Ok(())
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

    let mut result = conn_string
        .execute_sql_as_vec(&sql, &[], "get_db_fields", |db_row| TableColumn {
            name: db_row.get("column_name"),
            sql_type: get_sql_type(db_row),
            is_primary_key: None,
            is_nullable: get_is_nullable(db_row),
            default: None,
        })
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

    let result = conn_string
        .execute_sql_as_vec(&sql, &[], "get_db_fields", |db_row| {
            let result: String = db_row.get(0);
            result
        })
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
