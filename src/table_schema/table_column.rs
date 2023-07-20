use super::TableColumnType;

#[derive(Debug, Clone)]
pub struct TableColumn {
    pub name: String,
    pub sql_type: TableColumnType,
    pub is_nullable: bool,
    pub default: Option<&'static str>,
}

impl TableColumn {
    pub fn update_table_column(&mut self, table_name: &str, column: &TableColumn) {
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

    pub fn is_the_same_to(&self, other: &TableColumn) -> bool {
        if self.name != other.name {
            return false;
        }

        if !self.sql_type.equals_to(&other.sql_type) {
            return false;
        }

        if self.is_nullable != other.is_nullable {
            return false;
        }

        if self.default != other.default {
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
}
