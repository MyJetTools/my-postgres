use std::time::Duration;

use my_logger::LogEventCtx;
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use crate::{
    MyPostgresError, PostgresConnection, PostgresConnectionInstance, PostgresSettings,
    RequestContext,
};

const TECH_DB_NAME: &str = "postgres";

pub async fn check_if_db_exists(
    connection: &PostgresConnection,
    sql_timeout: Duration,
    #[cfg(feature = "with-ssh")] ssh_config: Option<crate::ssh::PostgresSshConfig>,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) {
    if let Err(error) = check_if_db_exists_int(
        connection,
        sql_timeout,
        #[cfg(feature = "with-logs-and-telemetry")]
        my_telemetry,
        #[cfg(feature = "with-ssh")]
        ssh_config,
    )
    .await
    {
        let mut ctx = std::collections::HashMap::new();

        ctx.insert("Err".to_string(), format!("{:?}", error));

        my_logger::LOGGER.write_info(
            "Table Existence verification",
            format!(
                "Can not execute script which checks DataBase existence. Err: {:?}",
                error
            ),
            LogEventCtx::new().add("db_name", connection.get_db_name().await),
        );
    }
}

async fn check_if_db_exists_int(
    connection: &PostgresConnection,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
    #[cfg(feature = "with-ssh")] ssh_config: Option<crate::ssh::PostgresSshConfig>,
) -> Result<(), MyPostgresError> {
    let (app_name, connection_string) = connection.get_connection_string().await;

    let tech_conn_string =
        connection_string.to_string_with_new_db_name(app_name.as_str(), TECH_DB_NAME);

    let tech_conn_string = TechConnectionStringProvider::new(tech_conn_string.as_str());

    let tech_connection = PostgresConnectionInstance::new(
        app_name.into(),
        TECH_DB_NAME.to_string(),
        std::sync::Arc::new(tech_conn_string),
        #[cfg(feature = "with-ssh")]
        ssh_config,
    )
    .await;

    let db_name = connection.get_db_name().await;

    println!("Checking that DB {} exists", db_name.as_str());
    let sql: String = format!("SELECT count(*) FROM pg_database WHERE datname='{db_name}'");

    let req_ctx = RequestContext::new(
        sql_timeout,
        format!("checking_if_db_exists {}", db_name),
        crate::is_debug("pg_database", "SELECT"),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(ctx),
    );
    let result: Option<usize> = tech_connection.get_count(&sql.into(), &req_ctx).await?;

    if let Some(count) = result {
        if count > 0 {
            println!("Database {db_name} is found. Checked...");
            return Ok(());
        }
    }

    println!("Database {db_name} not found. Creating it...");

    let sql: String = format!("CREATE DATABASE {db_name}");

    my_logger::LOGGER.write_debug(
        "check_if_db_exists".to_string(),
        format!("Creating table {db_name}"),
        LogEventCtx::new()
            .add("db_name", db_name.to_string())
            .add("sql", sql.to_string()),
    );

    let req_ctx = RequestContext::new(
        sql_timeout,
        format!("creating_db_{}", db_name),
        crate::is_debug("pg_database", "SELECT"),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(ctx),
    );

    tech_connection.execute_sql(&sql.into(), &req_ctx).await?;

    Ok(())
}

struct TechConnectionStringProvider(String);

impl TechConnectionStringProvider {
    pub fn new(conn_string: &str) -> Self {
        Self(conn_string.to_string())
    }
}

#[async_trait::async_trait]
impl PostgresSettings for TechConnectionStringProvider {
    async fn get_connection_string(&self) -> String {
        self.0.clone()
    }
}
