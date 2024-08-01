use std::time::Duration;

use crate::{MyPostgresError, PostgresConnection, PostgresConnectionInstance, PostgresSettings};

const TECH_DB_NAME: &str = "postgres";

pub async fn check_if_db_exists(connection: &PostgresConnection, sql_timeout: Duration) {
    if let Err(error) = check_if_db_exists_int(connection, sql_timeout).await {
        println!(
            "Can not execute script which checks DataBase existence. Error: {:?}",
            error
        );

        #[cfg(feature = "with-logs-and-telemetry")]
        {
            let mut ctx = std::collections::HashMap::new();

            ctx.insert("Err".to_string(), format!("{:?}", error));

            connection.get_logger().write_info(
                "Table Existence verification".into(),
                format!("Can not execute script which checks DataBase existence",),
                Some(ctx),
            );
        }
    }
}

async fn check_if_db_exists_int(
    connection: &PostgresConnection,
    sql_timeout: Duration,
) -> Result<(), MyPostgresError> {
    let (app_name, connection_string) = connection.get_connection_string().await;

    let tech_conn_string =
        connection_string.to_string_with_new_db_name(app_name.as_str(), TECH_DB_NAME);

    let tech_conn_string = TechConnectionStringProvider::new(tech_conn_string.as_str());

    let tech_connection = PostgresConnectionInstance::new(
        app_name.into(),
        TECH_DB_NAME.to_string(),
        std::sync::Arc::new(tech_conn_string),
        #[cfg(feature = "with-logs-and-telemetry")]
        connection.get_logger().clone(),
        #[cfg(feature = "with-ssh")]
        connection.clone_ssh_target(),
    )
    .await;

    let db_name = connection.get_db_name().await;

    println!("Checking that DB {} exists", db_name.as_str());
    let sql: String = format!("SELECT count(*) FROM pg_database WHERE datname='{db_name}'");

    let result: Option<usize> = tech_connection
        .get_count(
            &sql.into(),
            format!("checking_if_db_exists {}", db_name),
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await?;

    if let Some(count) = result {
        if count > 0 {
            println!("Database {db_name} is found. Checked...");
            return Ok(());
        }
    }

    println!("Database {db_name} not found. Creating it...");

    let sql: String = format!("CREATE DATABASE {db_name}");

    #[cfg(feature = "with-logs-and-telemetry")]
    {
        let mut ctx = std::collections::HashMap::new();

        ctx.insert("sql".to_string(), sql.to_string());
        connection.get_logger().write_warning(
            "check_if_db_exists".to_string(),
            format!("Creating table {db_name}"),
            Some(ctx),
        );
    }

    tech_connection
        .execute_sql(
            &sql.into(),
            format!("creating_db_{}", db_name),
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await?;

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
