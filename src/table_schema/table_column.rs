use super::TableColumnType;

pub struct ColumnIsTheSame {
    pub schema_is_different: bool,
    pub primary_key_is_different: bool,
}

#[derive(Debug, Clone)]
pub struct TableColumn {
    pub name: String,
    pub sql_type: TableColumnType,
    pub is_primary_key: Option<u8>,
    pub is_nullable: bool,
    pub default: Option<String>,
}

impl TableColumn {
    pub fn update_table_column(&mut self, table_name: &str, column: &TableColumn) {
        if !self.sql_type.equals_to(&column.sql_type) {
            panic!(
                "Two table models for the same table '{}' have different column types",
                table_name
            );
        }

        if let Some(order) = column.is_primary_key {
            self.is_primary_key = Some(order);
        }

        if column.is_nullable {
            self.is_nullable = true;
        }
    }

    pub fn is_the_same(&self, other: &TableColumn) -> ColumnIsTheSame {
        let mut schema_is_different = false;
        let mut primary_key_is_different = false;
        if self.name != other.name {
            schema_is_different = true;
        }

        if !self.sql_type.equals_to(&other.sql_type) {
            schema_is_different = true;
        }

        if self.is_nullable != other.is_nullable {
            schema_is_different = true;
        }

        if self.default != other.default {
            schema_is_different = true;
        }

        if self.is_primary_key != other.is_primary_key {
            primary_key_is_different = true;
        }

        ColumnIsTheSame {
            schema_is_different,
            primary_key_is_different,
        }
    }

    pub fn generate_is_nullable_sql(&self) -> &'static str {
        if self.is_nullable {
            "null"
        } else {
            "not null"
        }
    }
}
