use crate::SqlValueWrapper;

pub trait SqlInsertModel<'s> {
    fn get_fields_amount() -> usize;
    fn get_field_name(no: usize) -> &'static str;
    fn get_field_value(&'s self, no: usize) -> SqlValueWrapper<'s>;
}
