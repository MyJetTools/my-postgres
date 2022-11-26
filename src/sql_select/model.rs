use crate::OrderByFields;

pub trait BulkSelectEntity {
    fn get_line_no(&self) -> i32;
}

pub trait SelectEntity<'s> {
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
    fn get_select_fields() -> &'static str;
    fn get_order_by_fields() -> Option<OrderByFields<'s>>;
}
