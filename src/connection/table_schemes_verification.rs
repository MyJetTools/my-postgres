use std::{collections::HashMap, time::Duration};

use tokio_postgres::Row;

use crate::table_schema::{TableColumn, TableColumnType};

pub async fn verify_schemas(postgres_client: &tokio_postgres::Client) -> Result<(), String> {
    for (table_name, table_schema) in crate::TABLE_SCHEMAS.get_schemas_to_verify().await {
        let sql = format!(
            "SELECT column_name,is_nullable,data_type FROM information_schema.columns WHERE table_name = '{}'",
            table_name
        );
        let result = postgres_client.query(&sql, &[]);

        let result = tokio::time::timeout(Duration::from_secs(5), result).await;

        if result.is_err() {
            return Err(format!(
                "Timeout during schema verification for table {}",
                table_name
            ));
        }

        let result = result.unwrap();

        if let Err(err) = result {
            return Err(format!(
                "Error during schema verification for table {}: {:?}",
                table_name, err
            ));
        }

        let rows = result.unwrap();

        if rows.len() == 0 {
            return Err(format!("Table {} is not found in DB", table_name));
        }

        let mut schema_from_db = get_columns_from_schema(rows)?;

        update_primary_key(postgres_client, &table_name, &mut schema_from_db).await?;

        for column in table_schema.values() {
            if let Some(column_from_db) = schema_from_db.get(&column.name) {
                if column_from_db.sql_type.equals_to(&column.sql_type) {
                    println!(
                        "Column {} type in DB id: {:?}. Dto type {:?}",
                        column.name, column_from_db.sql_type, column.sql_type
                    );
                }

                if column_from_db.is_nullable != column.is_nullable {
                    println!(
                        "Column {} in DB is_nullable= {}. Dto type is_nullable={}",
                        column.name, column_from_db.is_nullable, column.is_nullable
                    );
                }

                if column_from_db.is_primary_key != column.is_primary_key {
                    println!(
                        "Column {} in DB primary_key= {}. Dto type primary_key={}",
                        column.name, column_from_db.is_nullable, column.is_nullable
                    );
                }
            } else {
                println!("Missing Column: {}", column.name);
            }
        }
    }

    Ok(())
}

fn get_columns_from_schema(rows: Vec<Row>) -> Result<HashMap<String, TableColumn>, String> {
    let mut result = HashMap::new();
    for row in rows {
        let data_type: String = row.get("data_type");
        let sql_type = TableColumnType::from_db_string(&data_type);

        if sql_type.is_none() {
            return Err(format!("Unknown data type: {}", data_type));
        }

        let name: String = row.get("column_name");
        let is_nullable: String = row.get("is_nullable");
        let column = TableColumn {
            name: name.to_string(),
            is_nullable: is_nullable == "YES",
            sql_type: sql_type.unwrap(),
            is_primary_key: None,
        };

        result.insert(name, column);
    }

    Ok(result)
}

async fn update_primary_key(
    postgres_client: &tokio_postgres::Client,
    table_name: &str,
    columns: &mut HashMap<String, TableColumn>,
) -> Result<(), String> {
    let sql = format!(
        r#"SELECT c.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{}'"#,
        table_name
    );

    let result = postgres_client.query(&sql, &[]);

    let result = tokio::time::timeout(Duration::from_secs(5), result).await;

    if result.is_err() {
        return Err(format!(
            "Timeout during schema verification for table {}",
            table_name
        ));
    }

    let result = result.unwrap();

    if let Err(err) = result {
        return Err(format!(
            "Error during schema verification for table {}: {:?}",
            table_name, err
        ));
    }
    let mut no = 0;

    for row in result.unwrap() {
        let column_name: String = row.get("column_name");
        if let Some(column) = columns.get_mut(&column_name) {
            column.is_primary_key = Some(no);
            no += 1;
        }
    }

    Ok(())
}
