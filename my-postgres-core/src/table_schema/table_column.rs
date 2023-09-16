use rust_extensions::StrOrString;

use crate::ColumnName;

use super::TableColumnType;

#[derive(Debug, Clone)]
pub struct TableColumn {
    pub name: ColumnName,
    pub sql_type: TableColumnType,
    pub is_nullable: bool,
    pub default: Option<StrOrString<'static>>,
}

impl TableColumn {
    pub fn update_table_column(&mut self, table_name: &str, column: &Self) {
        if !self.sql_type.equals_to(&column.sql_type) {
            panic!(
                "Two table models for the same table '{}' have different column types",
                table_name
            );
        }

        if column.is_nullable {
            self.is_nullable = true;
        }
    }

    pub fn is_the_same_to(&self, other: &Self) -> bool {
        if !self.sql_type.equals_to(&other.sql_type) {
            return false;
        }

        if self.is_nullable != other.is_nullable {
            return false;
        }

        if !self.is_default_the_same(other) {
            return false;
        }

        true
    }

    pub fn generate_is_nullable_sql(&self) -> &'static str {
        if self.is_nullable {
            "null"
        } else {
            "not null"
        }
    }

    pub fn is_default_the_same(&self, other: &Self) -> bool {
        if let Some(self_default) = &self.default {
            if let Some(other_default) = &other.default {
                return other_default.as_str() == self_default.as_str();
            }
        } else {
            if other.default.is_none() {
                return true;
            }
        }

        false
    }

    pub fn get_default(&self) -> Option<String> {
        let default_value = self.default.as_ref()?.as_str();

        match &self.sql_type {
            TableColumnType::Text => {
                if default_value.starts_with("'") {
                    return Some(default_value.to_string());
                } else {
                    return Some(format!("'{}'", default_value));
                }
            }
            TableColumnType::SmallInt => {
                return Some(default_value.to_string());
            }
            TableColumnType::BigInt => {
                return Some(default_value.to_string());
            }
            TableColumnType::Boolean => {
                return Some(default_value.to_string());
            }
            TableColumnType::Real => {
                return Some(default_value.to_string());
            }
            TableColumnType::Double => {
                return Some(default_value.to_string());
            }
            TableColumnType::Integer => {
                return Some(default_value.to_string());
            }
            TableColumnType::Json => {
                if default_value.starts_with("'") {
                    return Some(default_value.to_string());
                } else {
                    return Some(format!("'{}'", default_value));
                }
            }
            TableColumnType::Timestamp => {
                if default_value.starts_with("'") {
                    return Some(default_value.to_string());
                } else {
                    return Some(format!("'{}'", default_value));
                }
            }
            TableColumnType::Jsonb => {
                if default_value.starts_with("'") {
                    return Some(default_value.to_string());
                } else {
                    return Some(format!("'{}'", default_value));
                }
            }
        }
    }
}
