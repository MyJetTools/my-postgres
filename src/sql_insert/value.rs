use crate::SqlValue;

pub struct SqlInsertValue<'s> {
    pub name: &'static str,
    pub value: SqlValue<'s>,
}
