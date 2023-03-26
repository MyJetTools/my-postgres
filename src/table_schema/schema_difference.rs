use std::collections::HashMap;

use super::{TableColumn, TableSchema};

pub struct SchemaDifference {
    pub primary_key_is_different: bool,
    pub to_add: Vec<String>,
    pub to_update: Vec<String>,
}

impl SchemaDifference {
    pub fn new(table_schema: &TableSchema, db_fields: &HashMap<String, TableColumn>) -> Self {
        let mut to_add = Vec::new();
        let mut to_update = Vec::new();

        for schema_column in &table_schema.columns {
            if let Some(db_field) = db_fields.get(&schema_column.name) {
                if !db_field.is_the_same(schema_column) {
                    to_update.push(schema_column.name.clone());
                }
            } else {
                to_add.push(schema_column.name.clone());
            }
        }

        Self {
            primary_key_is_different: false,
            to_add,
            to_update,
        }
    }
}
