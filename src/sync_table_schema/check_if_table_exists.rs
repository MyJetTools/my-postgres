use crate::{MyPostgresError, PostgresConnection};

pub async fn check_if_db_exists(
    connection: &PostgresConnection,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &std::sync::Arc<
        dyn rust_extensions::Logger + Sync + Send + 'static,
    >,
) -> Result<(), MyPostgresError> {
    println!("Checking that DB exists");
    let db_name = connection.get_db_name().await;
    let sql: String = format!("SELECT count(*) FROM pg_database WHERE datname='{db_name}'");

    let result: Option<usize> = connection
        .get_count_low_level(
            &sql.into(),
            Some("checking_if_db_exists".into()),
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await?;

    if let Some(count) = result {
        if count > 0 {
            return Ok(());
        }
    }

    println!("Database {db_name} not found. Creating it...");

    let sql: String = format!("CREATE DATABASE {db_name}'");

    #[cfg(feature = "with-logs-and-telemetry")]
    {
        let mut ctx = std::collections::HashMap::new();

        ctx.insert("sql".to_string(), sql.to_string());
        logger.write_warning(
            "check_if_db_exists".to_string(),
            format!("Creating table {db_name}"),
            Some(ctx),
        );
    }

    connection
        .execute_sql(
            &sql.into(),
            None,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await?;

    Ok(())
}
