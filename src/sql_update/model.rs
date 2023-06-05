use super::SqlUpdateValue;

pub trait SqlUpdateModel<'s> {
    fn get_field_value(&'s self, no: usize) -> SqlUpdateValue<'s>;
    fn get_fields_amount() -> usize;

    fn get_e_tag_field_name(&self) -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);
}
