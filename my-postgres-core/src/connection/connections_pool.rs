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
    #[cfg(all(unix, feature = "with-ssh"))]
    pub ssh_config: Option<crate::ssh::PostgresSshConfig>,
}

impl MyPostgresFactory {
    pub fn new(
        app_name: StrOrString<'static>,
        db_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        #[cfg(all(unix, feature = "with-ssh"))] ssh_config: Option<crate::ssh::PostgresSshConfig>,
    ) -> Self {
        Self {
            postgres_settings,
            app_name,
            db_name,
            #[cfg(all(unix, feature = "with-ssh"))]
            ssh_config,
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
            #[cfg(all(unix, feature = "with-ssh"))]
            self.ssh_config.clone(),
        )
        .await
    }
}

pub struct ConnectionsPool {
    connections: ObjectsPool<PostgresConnectionInstance, MyPostgresFactory>,
    #[cfg(all(unix, feature = "with-ssh"))]
    pub ssh_config: Option<crate::ssh::PostgresSshConfig>,
}

impl ConnectionsPool {
    pub fn new(
        app_name: StrOrString<'static>,
        db_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        max_pool_size: usize,
        #[cfg(all(unix, feature = "with-ssh"))] ssh_config: Option<crate::ssh::PostgresSshConfig>,
    ) -> Self {
        Self {
            #[cfg(all(unix, feature = "with-ssh"))]
            ssh_config: ssh_config.clone(),
            connections: ObjectsPool::new(
                max_pool_size,
                Arc::new(MyPostgresFactory::new(
                    app_name.clone(),
                    db_name,
                    postgres_settings.clone(),
                    #[cfg(all(unix, feature = "with-ssh"))]
                    ssh_config,
                )),
            ),
        }
    }

    pub async fn get(&self) -> RentedObject<PostgresConnectionInstance> {
        self.connections.get_element().await
    }
}
