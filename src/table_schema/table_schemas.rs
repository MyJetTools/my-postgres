use std::collections::HashMap;

use tokio::sync::RwLock;

use super::{TableColumn, TableSchema};

pub struct TableSchemas {
    pub schemas: RwLock<HashMap<String, TableSchema>>,
}

impl TableSchemas {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_columns(
        &self,
        table_name: &'static str,
        partition_key_name: Option<String>,
        columns: Vec<TableColumn>,
    ) {
        let mut schemas = self.schemas.write().await;

        if !schemas.contains_key(table_name) {
            schemas.insert(
                table_name.to_string(),
                TableSchema::new(table_name, partition_key_name, columns),
            );
        }
    }

    pub async fn get_schemas(&self) -> HashMap<String, TableSchema> {
        let schemas = self.schemas.read().await;
        schemas.clone()
    }
}
