use std::sync::Arc;

use my_telemetry::MyTelemetryContext;
use rust_extensions::objects_pool::{ObjectsPool, RentedObject};

use crate::{
    DeleteEntity, InsertEntity, InsertOrUpdateEntity, MyPostgres, SelectEntity, UpdateEntity,
};

struct MyPostgresFactory {
    conn_string: String,
}

impl MyPostgresFactory {
    pub fn new(conn_string: String) -> Self {
        Self { conn_string }
    }
}

#[async_trait::async_trait]
impl rust_extensions::objects_pool::ObjectsPoolFactory<MyPostgres> for MyPostgresFactory {
    async fn create_new(&self) -> MyPostgres {
        MyPostgres::crate_no_tls(self.conn_string.as_str()).await
    }
}

pub struct ConnectionsPool {
    connections: ObjectsPool<MyPostgres, MyPostgresFactory>,
}

impl ConnectionsPool {
    pub fn no_tls(connection_string: String, max_pool_size: usize) -> Self {
        Self {
            connections: ObjectsPool::new(
                max_pool_size,
                Arc::new(MyPostgresFactory::new(connection_string)),
            ),
        }
    }

    pub async fn get_connection(&self) -> RentedObject<MyPostgres> {
        self.connections.get_element().await
    }

    pub async fn query_single_row<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<TEntity>, tokio_postgres::Error> {
        let connection = self.get_connection().await;
        let write_access = connection.value.lock().await;
        write_access
            .query_single_row(select, params, telemetry_context)
            .await
    }

    pub async fn query_rows<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, tokio_postgres::Error> {
        let connection = self.get_connection().await;
        let write_access = connection.value.lock().await;
        write_access
            .query_rows(select, params, telemetry_context)
            .await
    }

    pub async fn insert_db_entity<TEntity: InsertEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let connection = self.get_connection().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_db_entity(entity, table_name, telemetry_context)
            .await
    }

    pub async fn update_db_entity<TEntity: UpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let connection = self.get_connection().await;
        let write_access = connection.value.lock().await;
        write_access
            .update_db_entity(entity, table_name, telemetry_context)
            .await
    }

    pub async fn bulk_insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &self,
        entities: Vec<TEntity>,
        table_name: &str,
        pk_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let connection = self.get_connection().await;
        let mut write_access = connection.value.lock().await;
        write_access
            .bulk_insert_or_update_db_entity(entities, table_name, pk_name, telemetry_context)
            .await
    }

    pub async fn insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        pk_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let connection = self.get_connection().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_or_update_db_entity(entity, table_name, pk_name, telemetry_context)
            .await
    }

    pub async fn bulk_delete<TEntity: DeleteEntity>(
        &self,
        entities: Vec<TEntity>,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let connection = self.get_connection().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_delete(entities, table_name, telemetry_context)
            .await
    }
}
