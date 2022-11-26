pub trait BulkSelectEntity {
    fn get_line_no(&self) -> i32;
}

pub trait SelectEntity {
    fn from(row: &tokio_postgres::Row) -> Self;
    fn fill_select_fields(sql: &mut String);
    fn fill_order_by_fields(sql: &mut String);
    fn fill_group_by_fields(sql: &mut String);
}
