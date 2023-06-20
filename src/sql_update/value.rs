use crate::SqlUpdateValueWrapper;

pub struct SqlUpdateValue<'s> {
    pub name: &'static str,
    pub related_name: Option<&'static str>,
    pub value: SqlUpdateValueWrapper<'s>,
}
