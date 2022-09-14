use std::sync::Arc;

use my_telemetry::MyTelemetryContext;
use rust_extensions::{
    objects_pool::{ObjectsPool, RentedObject},
    Logger,
};

use crate::{
    DeleteEntity, InsertEntity, InsertOrUpdateEntity, MyPostgres, MyPostgressError,
    PostgressSettings, SelectEntity, UpdateEntity,
};

struct MyPostgresFactory {
    app_name: String,
    postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
    logger: Arc<dyn Logger + Sync + Send + 'static>,
}

impl MyPostgresFactory {
    pub fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            postgres_settings,
            app_name,
            logger,
        }
    }
}

#[async_trait::async_trait]
impl rust_extensions::objects_pool::ObjectsPoolFactory<MyPostgres> for MyPostgresFactory {
    async fn create_new(&self) -> MyPostgres {
        MyPostgres::new(
            self.app_name.clone(),
            self.postgres_settings.clone(),
            self.logger.clone(),
        )
        .await
    }
}

pub struct ConnectionsPool {
    connections: ObjectsPool<MyPostgres, MyPostgresFactory>,
}

impl ConnectionsPool {
    pub fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        max_pool_size: usize,
        logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            connections: ObjectsPool::new(
                max_pool_size,
                Arc::new(MyPostgresFactory::new(
                    app_name.clone(),
                    postgres_settings.clone(),
                    logger,
                )),
            ),
        }
    }

    pub async fn get_postgres_client(&self) -> RentedObject<MyPostgres> {
        self.connections.get_element().await
    }

    pub async fn get_count(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<i64>, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .get_count(select, params, telemetry_context)
            .await
    }

    pub async fn query_single_row<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .query_single_row(select, params, telemetry_context)
            .await
    }

    pub async fn query_rows<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .query_rows(select, params, telemetry_context)
            .await
    }

    pub async fn insert_db_entity<TEntity: InsertEntity>(
        &self,
        entity: &TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_db_entity(entity, table_name, telemetry_context)
            .await
    }

    pub async fn insert_db_entity_if_not_exists<TEntity: InsertEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_db_entity_if_not_exists(entity, table_name, telemetry_context)
            .await
    }

    pub async fn bulk_insert_db_entities<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_insert_db_entities(entities, table_name, telemetry_context)
            .await
    }

    pub async fn bulk_insert_db_entities_if_not_exists<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_insert_db_entities_if_not_exists(entities, table_name, telemetry_context)
            .await
    }

    pub async fn update_db_entity<TEntity: UpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
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
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
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
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_or_update_db_entity(entity, table_name, pk_name, telemetry_context)
            .await
    }

    pub async fn bulk_delete<TEntity: DeleteEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_delete(entities, table_name, telemetry_context)
            .await
    }
}
