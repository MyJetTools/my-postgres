#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
#[cfg(feature = "with-tls")]
use openssl::ssl::{SslConnector, SslMethod};
#[cfg(feature = "with-tls")]
use postgres_openssl::MakeTlsConnector;
use rust_extensions::date_time::DateTimeAsMicroseconds;
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::{
    future::Future,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{sync::RwLock, time::error::Elapsed};
use tokio_postgres::NoTls;

use crate::{
    DeleteEntity, InsertEntity, InsertOrUpdateEntity, MyPostgressError, PostgresConnection,
    PostgressSettings, SelectEntity, UpdateEntity,
};

pub struct MyPostgres {
    client: Arc<RwLock<Option<PostgresConnection>>>,
    sql_request_timeout: Duration,
}

impl MyPostgres {
    pub async fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let shared_connection = Arc::new(RwLock::new(None));
        tokio::spawn(do_connection(
            app_name,
            postgres_settings,
            shared_connection.clone(),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        ));

        Self {
            client: shared_connection,
            sql_request_timeout: Duration::from_secs(5),
        }
    }

    pub fn set_sql_request_timeout(mut self, timeout: Duration) -> Self {
        self.sql_request_timeout = timeout;
        self
    }

    pub async fn get_count(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<i64>, MyPostgressError> {
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                connection
                    .get_count(
                        select,
                        params,
                        #[cfg(feature = "with-logs-and-telemetry")]
                        telemetry_context,
                    )
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn query_single_row<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .query_single_row(
                    select,
                    params,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                )
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn execute_sql(
        &self,
        sql: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<u64, MyPostgressError> {
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.execute_sql(
                    sql.as_str(),
                    params,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(sql.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn query_rows<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        sql: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.query_rows(
                    sql.as_str(),
                    params,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(sql.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn insert_db_entity<TEntity: InsertEntity>(
        &self,
        entity: &TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name: String = format!("insert_db_entity into table {}", table_name);
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.insert_db_entity(
                    entity,
                    table_name,
                    process_name.as_str(),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn insert_db_entity_if_not_exists<TEntity: InsertEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("insert_db_entity_if_not_exists into table {}", table_name);
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.insert_db_entity_if_not_exists(
                    entity,
                    table_name,
                    process_name.as_str(),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn bulk_insert_db_entities<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("bulk_insert_db_entities into table {}", table_name);

        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.bulk_insert_db_entities(
                    entities,
                    table_name,
                    process_name.as_str(),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn bulk_insert_db_entities_if_not_exists<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!(
            "bulk_insert_db_entities_if_not_exists into table {}",
            table_name
        );
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.bulk_insert_db_entities_if_not_exists(
                    entities,
                    table_name,
                    &process_name,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn update_db_entity<TEntity: UpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("update_db_entity into table {}", table_name);
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.update_db_entity(
                    entity,
                    table_name,
                    &process_name,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn bulk_insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &self,
        entities: Vec<TEntity>,
        table_name: &str,
        pk_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!(
            "bulk_insert_or_update_db_entity into table {} {} entities",
            table_name,
            entities.len()
        );

        let result = {
            let mut write_access = self.client.write().await;

            if let Some(connection) = write_access.as_mut() {
                let execution = connection.bulk_insert_or_update_db_entity(
                    entities,
                    table_name,
                    pk_name,
                    &process_name,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        pk_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("insert_or_update_db_entity into table {}", table_name);

        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.insert_or_update_db_entity(
                    entity,
                    table_name,
                    pk_name,
                    &process_name,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    pub async fn bulk_delete<TEntity: DeleteEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("bulk_delete from table {}", table_name);
        let result = {
            let read_access = self.client.read().await;

            if let Some(connection) = read_access.as_ref() {
                let execution = connection.bulk_delete(
                    entities,
                    table_name,
                    &process_name,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    telemetry_context,
                );

                self.execute_request_with_timeout(process_name.as_str(), execution)
                    .await
            } else {
                Err(MyPostgressError::NoConnection)
            }
        };

        self.handle_error(result).await
    }

    async fn execute_request_with_timeout<
        TResult,
        TFuture: Future<Output = Result<TResult, MyPostgressError>>,
    >(
        &self,
        sql: &str,
        execution: TFuture,
    ) -> Result<TResult, MyPostgressError> {
        let timeout_result: Result<Result<TResult, MyPostgressError>, Elapsed> =
            tokio::time::timeout(self.sql_request_timeout, execution).await;

        if timeout_result.is_err() {
            println!(
                "{}: query_rows {} is timeouted after {:?}",
                DateTimeAsMicroseconds::now().to_rfc3339(),
                sql,
                self.sql_request_timeout
            );
            Err(MyPostgressError::Timeouted(self.sql_request_timeout))
        } else {
            timeout_result.unwrap()
        }
    }

    async fn handle_error<T>(
        &self,
        result: Result<T, MyPostgressError>,
    ) -> Result<T, MyPostgressError> {
        if let Err(err) = &result {
            if let MyPostgressError::Timeouted(_) = err {
                let mut write_access = self.client.write().await;
                if let Some(client) = write_access.take() {
                    client.disconnect();
                }
            }
        }

        result
    }
}

async fn do_connection(
    app_name: String,
    postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
    shared_connection: Arc<RwLock<Option<PostgresConnection>>>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
) {
    loop {
        let conn_string = postgres_settings.get_connection_string().await;

        let conn_string = super::connection_string::format(conn_string.as_str(), app_name.as_str());

        if conn_string.contains("sslmode=require") {
            #[cfg(feature = "with-tls")]
            create_and_start_with_tls(
                conn_string,
                &shared_connection,
                #[cfg(feature = "with-logs-and-telemetry")]
                &logger,
            )
            .await;
            #[cfg(not(feature = "with-tls"))]
            {
                #[cfg(feature = "with-logs-and-telemetry")]
                logger.write_error(
                    "PostgressConnection".to_string(),
                    "Postgres connection with sslmode=require is not supported without tls feature"
                        .to_string(),
                    None,
                );

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        } else {
            create_and_start_no_tls(
                conn_string,
                &shared_connection,
                #[cfg(feature = "with-logs-and-telemetry")]
                &logger,
            )
            .await
        }
    }
}

async fn create_and_start_no_tls(
    connection_string: String,
    shared_connection: &Arc<RwLock<Option<PostgresConnection>>>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let result = tokio_postgres::connect(connection_string.as_str(), NoTls).await;

    #[cfg(feature = "with-logs-and-telemetry")]
    let logger_spawned = logger.clone();
    match result {
        Ok((client, connection)) => {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            println!(
                "{}: Postgres SQL Connection is closed",
                DateTimeAsMicroseconds::now().to_rfc3339()
            );

            let connected = {
                let mut write_access = shared_connection.write().await;
                let postgress_connection = PostgresConnection::new(
                    client,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    logger.clone(),
                );
                let result = postgress_connection.connected.clone();
                *write_access = Some(postgress_connection);
                result
            };

            let connected_spawned = connected.clone();

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!(
                        "{}: connection error: {}",
                        DateTimeAsMicroseconds::now().to_rfc3339(),
                        e
                    );
                }
                #[cfg(feature = "with-logs-and-telemetry")]
                logger_spawned.write_fatal_error(
                    "Potgress background".to_string(),
                    format!("Exist connection loop"),
                    None,
                );

                connected_spawned.store(false, Ordering::SeqCst);
            });

            while connected.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(err) => {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            println!(
                "{}: Postgres SQL Connection is closed with Err: {:?}",
                DateTimeAsMicroseconds::now().to_rfc3339(),
                err
            );

            #[cfg(feature = "with-logs-and-telemetry")]
            logger.write_fatal_error(
                "CreatingPosrgress".to_string(),
                format!("Invalid connection string. {:?}", err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(feature = "with-tls")]
async fn create_and_start_with_tls(
    connection_string: String,
    shared_connection: &Arc<RwLock<Option<PostgresConnection>>>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let builder = SslConnector::builder(SslMethod::tls()).unwrap();

    let connector = MakeTlsConnector::new(builder.build());

    let result = tokio_postgres::connect(connection_string.as_str(), connector).await;
    #[cfg(feature = "with-logs-and-telemetry")]
    let logger_spawned = logger.clone();
    match result {
        Ok((client, connection)) => {
            let connected = {
                let mut write_access = shared_connection.write().await;
                let postgress_connection = PostgresConnection::new(
                    client,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    logger.clone(),
                );
                let result = postgress_connection.connected.clone();
                *write_access = Some(postgress_connection);
                result
            };

            let connected_copy = connected.clone();

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!(
                        "{}: connection error: {}",
                        DateTimeAsMicroseconds::now().to_rfc3339,
                        e
                    );
                }
                #[cfg(feature = "with-logs-and-telemetry")]
                logger_spawned.write_fatal_error(
                    "Potgress background".to_string(),
                    format!("Exist connection loop"),
                    None,
                );

                connected_copy.store(false, Ordering::SeqCst);
            });

            while connected.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(_err) => {
            #[cfg(feature = "with-logs-and-telemetry")]
            logger.write_fatal_error(
                "CreatingPosrgress".to_string(),
                format!("Invalid connection string. {:?}", _err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
