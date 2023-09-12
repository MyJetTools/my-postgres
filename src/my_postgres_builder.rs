use std::{sync::Arc, time::Duration};

use rust_extensions::{date_time::DateTimeAsMicroseconds, lazy::LazyVec, StrOrString};

use crate::{
    table_schema::{PrimaryKeySchema, TableSchema, TableSchemaProvider},
    MyPostgres, PostgresConnection, PostgresConnectionInstance, PostgresSettings,
};

pub enum MyPostgresBuilder {
    AsSettings {
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        app_name: String,
        table_schema_data: LazyVec<TableSchema>,
        sql_request_timeout: Duration,
        sql_db_sync_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")]
        logger: Arc<dyn rust_extensions::Logger + Send + Sync + 'static>,
    },
    AsSharedConnection {
        connection: Arc<PostgresConnection>,
        table_schema_data: LazyVec<TableSchema>,
        sql_request_timeout: Duration,
        sql_db_sync_timeout: Duration,
    },
}

impl MyPostgresBuilder {
    pub fn new(
        app_name: impl Into<StrOrString<'static>>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<
            dyn rust_extensions::Logger + Send + Sync + 'static,
        >,
    ) -> Self {
        let app_name: StrOrString<'static> = app_name.into();

        Self::AsSettings {
            app_name: app_name.to_string(),
            postgres_settings,
            table_schema_data: LazyVec::new(),
            sql_request_timeout: Duration::from_secs(5),
            sql_db_sync_timeout: Duration::from_secs(60),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        }
    }
    pub fn from_connection(connection: Arc<PostgresConnection>) -> Self {
        Self::AsSharedConnection {
            connection,
            table_schema_data: LazyVec::new(),
            sql_request_timeout: Duration::from_secs(5),
            sql_db_sync_timeout: Duration::from_secs(60),
        }
    }

    pub fn set_sql_request_timeout(mut self, value: Duration) -> Self {
        match &mut self {
            MyPostgresBuilder::AsSettings {
                sql_request_timeout,
                ..
            } => *sql_request_timeout = value,
            MyPostgresBuilder::AsSharedConnection {
                sql_request_timeout,
                ..
            } => *sql_request_timeout = value,
        }

        self
    }

    pub fn set_db_sync_timeout(mut self, value: Duration) -> Self {
        match &mut self {
            MyPostgresBuilder::AsSettings {
                sql_db_sync_timeout,
                ..
            } => *sql_db_sync_timeout = value,
            MyPostgresBuilder::AsSharedConnection {
                sql_db_sync_timeout,
                ..
            } => *sql_db_sync_timeout = value,
        }

        self
    }

    pub fn with_table_schema_verification<TTableSchemaProvider: TableSchemaProvider>(
        mut self,
        table_name: &'static str,
        primary_key_name: Option<String>,
    ) -> Self {
        let primary_key = if let Some(primary_key_name) = primary_key_name {
            if let Some(primary_key_columns) = TTableSchemaProvider::get_primary_key_columns() {
                Some((
                    primary_key_name,
                    PrimaryKeySchema::from_vec(primary_key_columns),
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

        let table_schema = TableSchema {
            table_name,
            primary_key,
            columns: TTableSchemaProvider::get_columns(),
            indexes: TTableSchemaProvider::get_indexes(),
        };

        match &mut self {
            MyPostgresBuilder::AsSettings {
                table_schema_data, ..
            } => table_schema_data.add(table_schema),
            MyPostgresBuilder::AsSharedConnection {
                table_schema_data, ..
            } => table_schema_data.add(table_schema),
        }

        self
    }

    pub async fn build(self) -> MyPostgres {
        match self {
            MyPostgresBuilder::AsSettings {
                postgres_settings,
                app_name,
                table_schema_data,

                sql_request_timeout,
                sql_db_sync_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                logger,
            } => {
                let connection = PostgresConnectionInstance::new(
                    app_name,
                    postgres_settings,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    logger,
                )
                .await;

                let connection = Arc::new(PostgresConnection::Single(connection));

                if let Some(table_schema_data) = table_schema_data.get_result() {
                    for table_schema in table_schema_data {
                        check_table_schema(&connection, table_schema, sql_db_sync_timeout).await;
                    }
                }

                MyPostgres::create(connection, sql_request_timeout)
            }
            MyPostgresBuilder::AsSharedConnection {
                connection,
                table_schema_data,
                sql_request_timeout,
                sql_db_sync_timeout,
            } => {
                if let Some(table_schema_data) = table_schema_data.get_result() {
                    for table_schema in table_schema_data {
                        check_table_schema(&connection, table_schema, sql_db_sync_timeout).await;
                    }
                }
                MyPostgres::create(connection, sql_request_timeout)
            }
        }
    }
}

pub async fn check_table_schema(
    connection: &PostgresConnection,
    table_schema: TableSchema,
    sql_timeout: Duration,
) {
    let started = DateTimeAsMicroseconds::now();

    while let Err(err) =
        crate::sync_table_schema::sync_schema(connection, &table_schema, sql_timeout).await
    {
        println!(
            "Can not verify schema for table {} because of error {:?}",
            table_schema.table_name, err
        );

        if DateTimeAsMicroseconds::now()
            .duration_since(started)
            .as_positive_or_zero()
            > Duration::from_secs(20)
        {
            panic!(
                "Aborting  the process due to the failing to verify table {} schema during 20 seconds.",
                table_schema.table_name
            );
        } else {
            println!("Retrying in 3 seconds...");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}
