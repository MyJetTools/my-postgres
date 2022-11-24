use super::SqlWhereValue;

pub trait SqlWhereModel<'s> {
    fn get_field_value(&'s self, no: usize) -> SqlWhereValue<'s>;
    fn get_fields_amount() -> usize;
}