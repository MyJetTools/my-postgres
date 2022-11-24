use crate::SqlValueWriter;

pub struct SqlUpdateValue<'s> {
    pub name: &'static str,
    pub value: Option<&'s dyn SqlValueWriter<'s>>,
}
