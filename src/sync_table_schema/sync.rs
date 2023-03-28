use crate::{MyPostgresError, PostgresConnection};

use crate::table_schema::TableSchema;

pub async fn sync_schema(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &std::sync::Arc<
        dyn rust_extensions::Logger + Sync + Send + 'static,
    >,
) -> Result<(), MyPostgresError> {
    loop {
        if super::sync_table_fields(
            conn_string,
            table_schema,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        )
        .await?
        {
            continue;
        }

        if super::sync_primary_key(
            conn_string,
            table_schema,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        )
        .await?
        {
            continue;
        }

        if super::sync_indexes(
            conn_string,
            table_schema,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        )
        .await?
        {
            continue;
        }

        #[cfg(not(feature = "with-logs-and-telemetry"))]
        println!(
            "Db Schema is up to date for a table: {}",
            table_schema.table_name
        );

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

        return Ok(());
    }
}
