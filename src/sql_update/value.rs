use crate::SqlValue;

pub struct SqlUpdateValue<'s> {
    pub name: &'static str,
    pub value: SqlValue<'s>,
}
