use super::TableColumn;

pub trait TableSchemaProvider {
    fn get_columns() -> Vec<TableColumn>;
}
