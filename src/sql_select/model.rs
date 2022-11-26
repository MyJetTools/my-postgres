use super::{GroupByFields, OrderByFields};

pub trait BulkSelectEntity {
    fn get_line_no(&self) -> i32;
}

pub enum FromDbRow<'s> {
    Single(&'s tokio_postgres::Row),
    Multiple(&'s [&'s tokio_postgres::Row]),
}

pub trait SelectEntity {
    fn from(src: FromDbRow) -> Self;
    fn get_select_fields() -> &'static str;
    fn get_order_by_fields() -> Option<OrderByFields>;
    fn get_group_by_fields() -> Option<GroupByFields>;
}
