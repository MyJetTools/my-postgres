use std::{sync::Arc, time::Duration};

use rust_extensions::{date_time::DateTimeAsMicroseconds, StrOrString};

use crate::{
    table_schema::{PrimaryKeySchema, TableSchema, TableSchemaProvider},
    MyPostgres, PostgresConnection, PostgresConnectionInstance, PostgresSettings,
};

pub struct MyPostgresBuilder {
    connection: Arc<PostgresConnection>,
}

impl MyPostgresBuilder {
    pub async fn new(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    ) -> Self {
        let app_name: StrOrString<'static> = app_name.into();

        let connection = PostgresConnectionInstance::new(
            app_name,
            postgres_settings,
            Duration::from_secs(5),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger.clone(),
        )
        .await;

        Self {
            connection: Arc::new(PostgresConnection::Single(connection)),
        }
    }
    pub fn from_connection(connection: Arc<PostgresConnection>) -> Self {
        #[cfg(feature = "with-logs-and-telemetry")]
        let logger = connection.get_logger().clone();
        Self {
            connection,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        }
    }

    pub async fn with_table_schema_verification<TTableSchemaProvider: TableSchemaProvider>(
        self,
        table_name: &'static str,
        primary_key_name: Option<String>,
    ) -> MyPostgres {
        self.check_table_schema::<TTableSchemaProvider>(table_name, primary_key_name)
            .await;
        MyPostgres::create(self.connection)
    }

    pub async fn with_no_table_schema_verification(self) -> MyPostgres {
        MyPostgres::create(self.connection)
    }

    pub async fn check_table_schema<TTableSchemaProvider: TableSchemaProvider>(
        &self,
        table_name: &'static str,
        primary_key_name: Option<String>,
    ) {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let columns = TTableSchemaProvider::get_columns();

        let primary_key = if let Some(primary_key_name) = primary_key_name {
            if let Some(primary_key_columns) = TTableSchemaProvider::PRIMARY_KEY_COLUMNS {
                Some((
                    primary_key_name,
                    PrimaryKeySchema::from_vec_of_str(primary_key_columns),
                ))
            } else {
                panic!(
                    "Provided primary key name {}, but there are no primary key columns defined.",
                    primary_key_name
                )
            }
        } else {
            None
        };

        let indexes = TTableSchemaProvider::get_indexes();

        let table_schema = TableSchema::new(table_name, primary_key, columns, indexes);

        let started = DateTimeAsMicroseconds::now();

        while let Err(err) = crate::sync_table_schema::sync_schema(
            &self.connection,
            &table_schema,
            #[cfg(feature = "with-logs-and-telemetry")]
            &self.logger,
        )
        .await
        {
            println!(
                "Can not verify schema for table {} because of error {:?}",
                table_name, err
            );

            if DateTimeAsMicroseconds::now()
                .duration_since(started)
                .as_positive_or_zero()
                > Duration::from_secs(20)
            {
                panic!(
                    "Aborting  the process due to the failing to verify table {} schema during 20 seconds.",
                    table_name
                );
            } else {
                println!("Retrying in 3 seconds...");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
