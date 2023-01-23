use crate::SqlUpdateValueWrapper;

pub struct SqlUpdateValue<'s> {
    pub name: &'static str,
    pub value: SqlUpdateValueWrapper<'s>,
}
