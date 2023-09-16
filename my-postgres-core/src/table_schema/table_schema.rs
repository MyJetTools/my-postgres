use core::panic;
use std::collections::HashMap;

use crate::ColumnName;

use super::{IndexSchema, PrimaryKeySchema, TableColumn, DEFAULT_SCHEMA};

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: &'static str,
    pub primary_key: Option<(String, PrimaryKeySchema)>,
    pub columns: Vec<TableColumn>,
    pub indexes: Option<HashMap<String, IndexSchema>>,
}

impl TableSchema {
    pub fn new(
        table_name: &'static str,
        primary_key: Option<(String, PrimaryKeySchema)>,

        columns: Vec<TableColumn>,
        indexes: Option<HashMap<String, IndexSchema>>,
    ) -> Self {
        Self {
            table_name,
            primary_key,
            columns,
            indexes,
        }
    }

    pub fn generate_create_table_script(&self) -> String {
        let mut result = String::new();
        result.push_str("create table ");
        result.push_str(DEFAULT_SCHEMA);
        result.push_str(".");
        result.push_str(self.table_name);
        result.push_str("\n(\n");

        let mut no = 0;

        for column in &self.columns {
            if no > 0 {
                result.push_str(",\n");
            }
            result.push_str("  ");
            column.name.push_name(&mut result);
            result.push_str(" ");
            result.push_str(column.sql_type.to_db_type());

            if let Some(default) = column.get_default() {
                result.push_str(" default ");
                result.push_str(default.as_str());
            }

            result.push_str(" ");
            result.push_str(column.generate_is_nullable_sql());

            no += 1;
        }

        if let Some((primary_key_name, primary_key_schema)) = &self.primary_key {
            result.push_str(",\n");
            result.push_str("  constraint ");
            result.push_str(primary_key_name);
            result.push_str("\n    primary key (");

            if let Some(primary_key_columns) = primary_key_schema.generate_primary_key_sql_columns()
            {
                result.push_str(primary_key_columns.as_str());
            } else {
                panic!(
                    "Somehow primary key {} columns were not found in table {} schema",
                    primary_key_name, &self.table_name
                )
            }

            result.push_str(")");
        } else {
            panic!(
                "Table {} does not have primary key name in Table Schema definition",
                self.table_name
            );
        }

        result.push_str(");");

        result
    }

    pub fn generate_add_column_sql(&self, column_name: &ColumnName) -> String {
        if let Some(column) = self
            .columns
            .iter()
            .find(|itm| itm.name.name.as_str() == column_name.name.as_str())
        {
            let schema = DEFAULT_SCHEMA;
            let table_name = self.table_name;
            let column_name = column.name.to_string();
            let column_type = column.sql_type.to_db_type();
            let default = if let Some(default) = column.get_default() {
                format!("default {}", default.as_str())
            } else {
                "".to_string()
            };
            let is_nullable = column.generate_is_nullable_sql();
            return format!(
                "alter table {schema}.{table_name} add {column_name} {column_type} {default} {is_nullable};"
            );
        }

        panic!(
            "Somehow column {} was not found in table schema",
            column_name.to_string()
        )
    }
}
