use std::collections::HashMap;

use tokio::sync::RwLock;

use super::TableColumn;

pub struct TableSchemas {
    pub schemas: RwLock<HashMap<String, HashMap<String, TableColumn>>>,
}

impl TableSchemas {
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_columns(&self, table_name: &str, columns: Vec<TableColumn>) {
        let mut schemas = self.schemas.write().await;

        if !schemas.contains_key(table_name) {
            schemas.insert(table_name.to_string(), HashMap::new());
        }

        let table_schema_inner = schemas.get_mut(table_name).unwrap();

        for column in columns {
            if let Some(table_column) = table_schema_inner.get_mut(&column.name) {
                table_column.update_table_column(table_name, &column);
            } else {
                table_schema_inner.insert(column.name.to_string(), column);
            }
        }
    }

    pub async fn get_schemas_to_verify(&self) -> HashMap<String, HashMap<String, TableColumn>> {
        let schemas = self.schemas.read().await;
        schemas.clone()
    }
}
