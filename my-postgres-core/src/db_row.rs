pub type DbRow = tokio_postgres::Row;

#[derive(Debug, Clone, Copy)]
pub struct DbColumnName {
    pub field_name: &'static str,
    pub db_column_name: &'static str,
    pub force_cast_db_type: bool,
}
