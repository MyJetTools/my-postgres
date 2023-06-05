use crate::SqlUpdateValueWrapper;

pub trait SqlInsertModel<'s> {
    fn get_fields_amount() -> usize;
    fn get_field_name(no: usize) -> &'static str;
    fn get_field_value(&'s self, no: usize) -> SqlUpdateValueWrapper<'s>;

    fn get_e_tag_field_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);
}
