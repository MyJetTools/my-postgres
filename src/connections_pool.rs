#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
use rust_extensions::objects_pool::{ObjectsPool, RentedObject};
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::sync::Arc;

use crate::{
    count_result::CountResult,
    sql_insert::SqlInsertModel,
    sql_select::{BulkSelectBuilder, BulkSelectEntity, SelectEntity, ToSqlString},
    sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel,
    MyPostgres, MyPostgressError, PostgressSettings,
};

struct MyPostgresFactory {
    app_name: String,
    postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Sync + Send + 'static>,
}

impl MyPostgresFactory {
    pub fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            postgres_settings,
            app_name,
            #[cfg(feature = "with-logs-and-telemetry")]
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
            #[cfg(feature = "with-logs-and-telemetry")]
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
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            connections: ObjectsPool::new(
                max_pool_size,
                Arc::new(MyPostgresFactory::new(
                    app_name.clone(),
                    postgres_settings.clone(),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    logger,
                )),
            ),
        }
    }

    pub async fn get_postgres_client(&self) -> RentedObject<MyPostgres> {
        self.connections.get_element().await
    }

    pub async fn get_count<'s, TWhereModel: SqlWhereModel<'s>, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .get_count(
                table_name,
                where_model,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_sql<ToSql: ToSqlString>(
        &self,
        sql: &ToSql,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;

        write_access
            .execute_sql(
                sql,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_single_row<
        's,
        TEntity: SelectEntity<'s> + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .query_single_row(
                table_name,
                where_model,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows<
        's,
        TEntity: SelectEntity<'s> + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .query_rows(
                table_name,
                where_model,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_query_rows_with_transformation<
        's,
        TIn: SqlWhereModel<'s> + Send + Sync + 'static,
        TOut,
        TEntity: SelectEntity<'s> + BulkSelectEntity + Send + Sync + 'static,
        TTransform: Fn(&TIn, Option<TEntity>) -> TOut,
    >(
        &self,
        sql_builder: &'s BulkSelectBuilder<'s, TIn>,

        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] ctx: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TOut>, MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;

        write_access
            .bulk_query_rows_with_transformation(
                sql_builder,
                transform,
                #[cfg(feature = "with-logs-and-telemetry")]
                ctx,
            )
            .await
    }

    pub async fn insert_db_entity<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_db_entity(
                entity,
                table_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_db_entity_if_not_exists<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_db_entity_if_not_exists(
                entity,
                table_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_db_entities<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entities: &'s [TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_insert_db_entities(
                entities,
                table_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_db_entities_if_not_exists<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        table_name: &str,
        entities: &'s [TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_insert_db_entities_if_not_exists(
                table_name,
                entities,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn update_db_entity<'s, TEntity: SqlUpdateModel<'s> + SqlWhereModel<'s>>(
        &self,
        table_name: &str,
        entity: &'s TEntity,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .update_db_entity(
                entity,
                table_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn delete<'s, TWhereModel: SqlWhereModel<'s>>(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,

        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .delete_db_entity(
                table_name,
                where_model,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    >(
        &self,
        table_name: &str,
        pk_name: &str,
        entities: &'s [TEntity],

        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_insert_or_update_db_entity(
                table_name,
                pk_name,
                entities,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    >(
        &self,
        table_name: &str,
        pk_name: &str,
        entity: &'s TEntity,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .insert_or_update_db_entity(
                table_name,
                pk_name,
                entity,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_delete<'s, TEntity: SqlWhereModel<'s>>(
        &self,
        entities: &'s [TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let connection = self.get_postgres_client().await;
        let write_access = connection.value.lock().await;
        write_access
            .bulk_delete(
                table_name,
                entities,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }
}
