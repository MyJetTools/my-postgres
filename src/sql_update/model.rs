use super::SqlUpdateValue;

pub trait SqlUpdateModel<'s> {
    fn get_field_value(&'s self, no: usize) -> SqlUpdateValue<'s>;
    fn get_fields_amount() -> usize;
}
