use crate::SqlValueWrapper;

pub struct SqlUpdateValue<'s> {
    pub name: &'static str,
    pub value: SqlValueWrapper<'s>,
}
