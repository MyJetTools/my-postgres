#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use rust_extensions::{
    objects_pool::{ObjectsPool, RentedObject},
    StrOrString,
};
use std::sync::Arc;

use crate::{PostgresConnectionInstance, PostgresSettings};

struct MyPostgresFactory {
    app_name: StrOrString<'static>,
    db_name: String,
    postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    #[cfg(feature = "with-ssh")]
    pub ssh_target: Arc<crate::ssh::SshTarget>,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Sync + Send + 'static>,
}

impl MyPostgresFactory {
    pub fn new(
        app_name: StrOrString<'static>,
        db_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-ssh")] ssh_target: Arc<crate::ssh::SshTarget>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            postgres_settings,
            app_name,
            db_name,
            #[cfg(feature = "with-ssh")]
            ssh_target,
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
            self.app_name.as_str().to_string(),
            self.db_name.clone(),
            self.postgres_settings.clone(),
            #[cfg(feature = "with-ssh")]
            self.ssh_target.clone(),
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
    #[cfg(feature = "with-ssh")]
    pub ssh_target: Arc<crate::ssh::SshTarget>,
}

impl ConnectionsPool {
    pub fn new(
        app_name: StrOrString<'static>,
        db_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        max_pool_size: usize,
        #[cfg(feature = "with-ssh")] ssh_target: Arc<crate::ssh::SshTarget>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        Self {
            #[cfg(feature = "with-logs-and-telemetry")]
            logger: logger.clone(),
            #[cfg(feature = "with-ssh")]
            ssh_target: ssh_target.clone(),
            connections: ObjectsPool::new(
                max_pool_size,
                Arc::new(MyPostgresFactory::new(
                    app_name.clone(),
                    db_name,
                    postgres_settings.clone(),
                    #[cfg(feature = "with-ssh")]
                    ssh_target,
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
