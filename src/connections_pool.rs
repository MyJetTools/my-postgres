use std::sync::Arc;

use rust_extensions::objects_pool::{ObjectsPool, RentedObject};

use crate::MyPostgres;

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
}
