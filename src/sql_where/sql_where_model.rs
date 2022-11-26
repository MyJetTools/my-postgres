use super::SqlWhereValue;

pub enum OrderByFields<'s> {
    Asc(Vec<&'s str>),
    Desc(Vec<&'s str>),
}

pub trait SqlWhereModel<'s> {
    fn get_field_value(&'s self, no: usize) -> SqlWhereValue<'s>;
    fn get_fields_amount() -> usize;
    fn get_order_by_fields() -> Option<OrderByFields<'s>>;
}
