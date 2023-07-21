#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use rust_extensions::{
    objects_pool::{ObjectsPool, RentedObject},
    StrOrString,
};
use std::{sync::Arc, time::Duration};

use crate::{PostgresConnectionInstance, PostgresSettings};

struct MyPostgresFactory {
    app_name: StrOrString<'static>,
    postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    exec_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Sync + Send + 'static>,
}

impl MyPostgresFactory {
    pub fn new(
        app_name: StrOrString<'static>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        exec_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            postgres_settings,
            app_name,
            exec_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        }
    }
}

#[async_trait::async_trait]
impl rust_extensions::objects_pool::ObjectsPoolFactory<PostgresConnectionInstance>
    for MyPostgresFactory
{
    async fn create_new(&self) -> PostgresConnectionInstance {
        PostgresConnectionInstance::new(
            self.app_name.clone(),
            self.postgres_settings.clone(),
            self.exec_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            self.logger.clone(),
        )
        .await
    }
}

pub struct ConnectionsPool {
    connections: ObjectsPool<PostgresConnectionInstance, MyPostgresFactory>,
    #[cfg(feature = "with-logs-and-telemetry")]
    pub logger: Arc<dyn Logger + Sync + Send + 'static>,
}

impl ConnectionsPool {
    pub fn new(
        app_name: StrOrString<'static>,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        max_pool_size: usize,
        request_timeout: Duration,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            #[cfg(feature = "with-logs-and-telemetry")]
            logger: logger.clone(),
            connections: ObjectsPool::new(
                max_pool_size,
                Arc::new(MyPostgresFactory::new(
                    app_name.clone(),
                    postgres_settings.clone(),
                    request_timeout,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    logger,
                )),
            ),
        }
    }

    pub async fn get(&self) -> RentedObject<PostgresConnectionInstance> {
        self.connections.get_element().await
    }
}
