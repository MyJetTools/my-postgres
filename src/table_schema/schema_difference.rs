use std::collections::HashMap;

use super::{TableColumn, TableSchema};

#[derive(Debug, Clone)]
pub struct ColumnDifference {
    pub db: TableColumn,
    pub required: TableColumn,
}

pub struct SchemaDifference {
    pub primary_key_is_different: bool,
    pub to_add: Vec<String>,
    pub to_update: Vec<ColumnDifference>,
}

impl SchemaDifference {
    pub fn new(table_schema: &TableSchema, db_fields: &HashMap<String, TableColumn>) -> Self {
        let mut to_add = Vec::new();
        let mut to_update = Vec::new();

        let mut primary_key_is_different = false;

        for schema_column in &table_schema.columns {
            if let Some(db_field) = db_fields.get(&schema_column.name) {
                let difference = db_field.is_the_same(schema_column);
                if difference.schema_is_different {
                    to_update.push(ColumnDifference {
                        db: db_field.clone(),
                        required: schema_column.clone(),
                    });
                }

                if difference.primary_key_is_different {
                    primary_key_is_different = true;
                }
            } else {
                to_add.push(schema_column.name.clone());
            }
        }

        Self {
            primary_key_is_different,
            to_add,
            to_update,
        }
    }
}
