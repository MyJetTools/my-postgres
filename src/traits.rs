pub trait BulkSelectEntity {
    fn get_line_no(&self) -> i32;
}

pub trait SelectEntity {
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
    fn get_select_fields() -> &'static str;
}
