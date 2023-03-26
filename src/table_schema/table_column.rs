use super::TableColumnType;

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
}
