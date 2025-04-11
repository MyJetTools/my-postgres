use std::time::Duration;

use my_logger::LogEventCtx;
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use crate::{MyPostgresError, PostgresConnection};

use crate::table_schema::TableSchema;

pub async fn sync_schema(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) -> Result<(), MyPostgresError> {
    loop {
        println!("--------------------------------------------------");
        println!("Syncing table schema: {}", table_schema.table_name);
        if std::env::var("DEBUG_SQL").is_ok() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        if super::sync_table_fields(
            conn_string,
            table_schema,
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry,
        )
        .await?
        {
            continue;
        }

        if super::sync_primary_key(
            conn_string,
            table_schema,
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry,
        )
        .await?
        {
            continue;
        }

        if super::sync_indexes(
            conn_string,
            table_schema,
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry,
        )
        .await?
        {
            continue;
        }

        my_logger::LOGGER.write_info(
            "Table Schema verification",
            format!(
                "Db Schema is up to date for a table, {}",
                table_schema.table_name
            ),
            LogEventCtx::new().add("table_name", table_schema.table_name),
        );

        println!(
            "Synchronization iteration for table schema {} is finished",
            table_schema.table_name
        );

        return Ok(());
    }
}
