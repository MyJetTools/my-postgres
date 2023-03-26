use std::collections::BTreeMap;

use super::{TableColumn, DEFAULT_SCHEMA};

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: &'static str,
    pub primary_key_name: Option<String>,
    pub columns: Vec<TableColumn>,
}

impl TableSchema {
    pub fn new(
        table_name: &'static str,
        primary_key_name: Option<String>,
        columns: Vec<TableColumn>,
    ) -> Self {
        Self {
            table_name,
            primary_key_name,
            columns,
        }
    }

    pub fn get_primary_key_fields(&self) -> Option<Vec<TableColumn>> {
        let mut by_primary_key = BTreeMap::new();
        let mut amount = 0;

        for table_column in &self.columns {
            if let Some(primary_key) = table_column.is_primary_key {
                if !by_primary_key.contains_key(&primary_key) {
                    by_primary_key.insert(primary_key, Vec::new());
                }

                by_primary_key
                    .get_mut(&primary_key)
                    .unwrap()
                    .push(table_column.clone());

                amount += 1;
            }
        }

        if by_primary_key.is_empty() {
            return None;
        }

        let mut result = Vec::with_capacity(amount);

        for (_, columns) in by_primary_key {
            result.extend(columns);
        }

        Some(result)
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
            result.push_str(column.name.as_str());
            result.push_str(" ");
            result.push_str(column.sql_type.to_db_type());
            result.push_str(" ");
            result.push_str(if column.is_nullable {
                "null"
            } else {
                "not null"
            });

            no += 1;
        }

        if let Some(primary_key_name) = self.primary_key_name.as_ref() {
            if let Some(primary_key_columns) = self.get_primary_key_fields() {
                result.push_str(",\n");
                result.push_str("  constraint ");
                result.push_str(primary_key_name);
                result.push_str("\n    primary key (");
                let mut no = 0;
                for column in primary_key_columns {
                    if no > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(column.name.as_str());
                    no += 1;
                }
                result.push_str(")");
            } else {
                panic!(
                "Table {} with primary key {} does not have primary key columns in Table Schema definition",
                self.table_name, primary_key_name
            );
            }
        }

        result.push_str(");");

        result
    }
}
