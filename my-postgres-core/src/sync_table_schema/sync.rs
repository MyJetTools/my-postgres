use std::time::Duration;

use crate::{MyPostgresError, PostgresConnection};

use crate::table_schema::TableSchema;

pub async fn sync_schema(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    sql_timeout: Duration,
) -> Result<(), MyPostgresError> {
    if let Err(error) = super::check_if_db_exists(conn_string, sql_timeout).await {
        println!(
            "Can not execute script which checks DataBase existence. Error: {:?}",
            error
        );

        #[cfg(feature = "with-logs-and-telemetry")]
        {
            let mut ctx = std::collections::HashMap::new();

            ctx.insert("Err".to_string(), format!("{:?}", error));

            conn_string.get_logger().write_info(
                "Table Existence verification".into(),
                format!("Can not execute script which checks DataBase existence",),
                Some(ctx),
            );
        }
    }

    loop {
        println!("--------------------------------------------------");
        println!("Syncing table schema: {}", table_schema.table_name);
        if std::env::var("DEBUG").is_ok() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        if super::sync_table_fields(conn_string, table_schema, sql_timeout).await? {
            continue;
        }

        if super::sync_primary_key(conn_string, table_schema, sql_timeout).await? {
            continue;
        }

        if super::sync_indexes(conn_string, table_schema, sql_timeout).await? {
            continue;
        }

        #[cfg(not(feature = "with-logs-and-telemetry"))]
        println!(
            "Db Schema is up to date for a table: {}",
            table_schema.table_name
        );

        #[cfg(feature = "with-logs-and-telemetry")]
        {
            conn_string.get_logger().write_info(
                "Table Schema verification".into(),
                format!(
                    "Db Schema is up to date for a table, {}",
                    table_schema.table_name
                ),
                None,
            );
        }

        println!(
            "Synchronization iteration for table schema {} is finished",
            table_schema.table_name
        );

        return Ok(());
    }
}
