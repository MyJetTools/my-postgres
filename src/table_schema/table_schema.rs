use super::TableColumn;

pub trait TableSchema {
    fn get_columns() -> Vec<TableColumn>;
}
