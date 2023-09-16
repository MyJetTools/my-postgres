use std::collections::HashMap;

use crate::ColumnName;

use super::{TableColumn, TableSchema};

#[derive(Debug, Clone)]
pub struct ColumnDifference {
    pub db: TableColumn,
    pub required: TableColumn,
}

pub struct SchemaDifference {
    pub to_add: Vec<ColumnName>,
    pub to_update: Vec<ColumnDifference>,
}

impl SchemaDifference {
    pub fn new(table_schema: &TableSchema, db_fields: &HashMap<String, TableColumn>) -> Self {
        let mut to_add = Vec::new();
        let mut to_update = Vec::new();

        for schema_column in &table_schema.columns {
            if let Some(db_field) = db_fields.get(schema_column.name.name.as_str()) {
                if !db_field.is_the_same_to(schema_column) {
                    to_update.push(ColumnDifference {
                        db: db_field.clone(),
                        required: schema_column.clone(),
                    });
                }
            } else {
                to_add.push(schema_column.name.clone());
            }
        }

        Self { to_add, to_update }
    }
}
