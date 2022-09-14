use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use my_telemetry::MyTelemetryContext;
#[cfg(feature = "with-tls")]
use openssl::ssl::{SslConnector, SslMethod};
#[cfg(feature = "with-tls")]
use postgres_openssl::MakeTlsConnector;
use rust_extensions::Logger;
use tokio::sync::RwLock;
use tokio_postgres::NoTls;

use crate::{
    DeleteEntity, InsertEntity, InsertOrUpdateEntity, MyPostgressError, PostgresConnection,
    PostgressSettings, SelectEntity, UpdateEntity,
};

pub struct MyPostgres {
    client: Arc<RwLock<Option<PostgresConnection>>>,
}

impl MyPostgres {
    pub async fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let shared_connection = Arc::new(RwLock::new(None));
        tokio::spawn(do_connection(
            app_name,
            postgres_settings,
            shared_connection.clone(),
            logger,
        ));

        Self {
            client: shared_connection,
        }
    }

    pub async fn get_count(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<i64>, MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .get_count(select, params, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn query_single_row<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .query_single_row(select, params, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn query_rows<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .query_rows(select, params, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn insert_db_entity<TEntity: InsertEntity>(
        &self,
        entity: &TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .insert_db_entity(entity, table_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn insert_db_entity_if_not_exists<TEntity: InsertEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .insert_db_entity_if_not_exists(entity, table_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn bulk_insert_db_entities<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .bulk_insert_db_entities(entities, table_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn bulk_insert_db_entities_if_not_exists<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .bulk_insert_db_entities_if_not_exists(entities, table_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn update_db_entity<TEntity: UpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .update_db_entity(entity, table_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn bulk_insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &self,
        entities: Vec<TEntity>,
        table_name: &str,
        pk_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let mut write_access = self.client.write().await;

        if let Some(connection) = write_access.as_mut() {
            connection
                .bulk_insert_or_update_db_entity(entities, table_name, pk_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        pk_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .insert_or_update_db_entity(entity, table_name, pk_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }

    pub async fn bulk_delete<TEntity: DeleteEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let read_access = self.client.read().await;

        if let Some(connection) = read_access.as_ref() {
            connection
                .bulk_delete(entities, table_name, telemetry_context)
                .await
        } else {
            Err(MyPostgressError::NoConnection)
        }
    }
}

async fn do_connection(
    app_name: String,
    postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
    shared_connection: Arc<RwLock<Option<PostgresConnection>>>,
    logger: Arc<dyn Logger + Sync + Send + 'static>,
) {
    loop {
        let conn_string = postgres_settings.get_connection_string().await;

        let conn_string = super::connection_string::format(conn_string.as_str(), app_name.as_str());

        if conn_string.contains("sslmode=require") {
            #[cfg(feature = "with-tls")]
            create_and_start_with_tls(conn_string, &shared_connection, &logger).await;
            #[cfg(not(feature = "with-tls"))]
            {
                logger.write_error(
                    "PostgressConnection".to_string(),
                    "Postgres connection with sslmode=require is not supported without tls feature"
                        .to_string(),
                    None,
                );

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        } else {
            create_and_start_no_tls(conn_string, &shared_connection, &logger).await
        }
    }
}

async fn create_and_start_no_tls(
    connection_string: String,
    shared_connection: &Arc<RwLock<Option<PostgresConnection>>>,
    logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let result = tokio_postgres::connect(connection_string.as_str(), NoTls).await;

    let logger_spawned = logger.clone();
    match result {
        Ok((client, connection)) => {
            let connected = {
                let mut write_access = shared_connection.write().await;
                let postgress_connection = PostgresConnection::new(client, logger.clone());
                let result = postgress_connection.connected.clone();
                *write_access = Some(postgress_connection);
                result
            };

            let connected_spawned = connected.clone();

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }

                logger_spawned.write_fatal_error(
                    "Potgress background".to_string(),
                    format!("Exist connection loop"),
                    None,
                );

                connected_spawned.store(true, Ordering::SeqCst);
            });

            while connected.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(err) => {
            logger.write_fatal_error(
                "CreatingPosrgress".to_string(),
                format!("Invalid connection string. {:?}", err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn create_and_start_with_tls(
    connection_string: String,
    shared_connection: &Arc<RwLock<Option<PostgresConnection>>>,
    logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let builder = SslConnector::builder(SslMethod::tls()).unwrap();

    let connector = MakeTlsConnector::new(builder.build());

    let result = tokio_postgres::connect(connection_string.as_str(), connector).await;

    let logger_spawned = logger.clone();
    match result {
        Ok((client, connection)) => {
            let connected = {
                let mut write_access = shared_connection.write().await;
                let postgress_connection = PostgresConnection::new(client, logger.clone());
                let result = postgress_connection.connected.clone();
                *write_access = Some(postgress_connection);
                result
            };

            let connected_copy = connected.clone();

            tokio::spawn(async move {
                connected_copy.store(false, Ordering::SeqCst);
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }

                logger_spawned.write_fatal_error(
                    "Potgress background".to_string(),
                    format!("Exist connection loop"),
                    None,
                );
            });

            while connected.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(err) => {
            logger.write_fatal_error(
                "CreatingPosrgress".to_string(),
                format!("Invalid connection string. {:?}", err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
