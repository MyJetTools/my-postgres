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

    fn get_primary_key_fields(&self) -> Option<Vec<TableColumn>> {
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
            result.push_str(",\n");
            result.push_str("  constraint ");
            result.push_str(primary_key_name);
            result.push_str("\n    primary key (");
            result.push_str(self.generate_primary_key_sql_columns().as_str());
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

    pub fn generate_add_column_sql(&self, column_name: &str) -> String {
        if let Some(column) = self.columns.iter().find(|itm| itm.name == column_name) {
            let schema = DEFAULT_SCHEMA;
            let table_name = self.table_name;
            let column_name = column.name.as_str();
            let column_type = column.sql_type.to_db_type();
            return format!("alter table {schema}.{table_name} add {column_name} {column_type};");
        }

        panic!(
            "Somehow column {} was not found in table schema",
            column_name
        )
    }

    pub fn generate_update_primary_key_sql(&self, has_primary_key_in_db: bool) -> Vec<String> {
        if !has_primary_key_in_db {
            let schema = DEFAULT_SCHEMA;
            let table_name = self.table_name;
            let primary_key_columns = self.generate_primary_key_sql_columns();
            if let Some(primary_key) = &self.primary_key_name {
                return vec![format!(
                    "alter table {schema}.{table_name} add constraint {primary_key} primary key ({primary_key_columns});")
                ];
            } else {
                panic!(
                    "Somehow primary key was not found in table schema {}",
                    self.table_name
                );
            }
        }

        if let Some(primary_key) = &self.primary_key_name {
            let schema = DEFAULT_SCHEMA;
            let table_name = self.table_name;

            let mut result = Vec::with_capacity(2);
            result.push(format!(
                "alter table {schema}.{table_name} drop constraint {primary_key};"
            ));

            let primary_key_columns = self.generate_primary_key_sql_columns();
            result.push(format!(
                "alter table {schema}.{table_name} add constraint {primary_key} primary key ({primary_key_columns});"
            ));

            return result;
        }

        panic!(
            "Somehow primary key was not found in table schema {}",
            self.table_name
        )
    }

    fn generate_primary_key_sql_columns(&self) -> String {
        if let Some(primary_key_columns) = self.get_primary_key_fields() {
            let mut result = String::new();
            let mut no = 0;
            for column in primary_key_columns {
                if no > 0 {
                    result.push_str(", ");
                }
                result.push_str(column.name.as_str());
                no += 1;
            }

            return result;
        }

        panic!(
            "Somehow primary key columns was not found in table {} with primary_key {:?}",
            self.table_name, self.primary_key_name
        )
    }
}
