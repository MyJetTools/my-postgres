use rust_extensions::StrOrString;

use crate::SqlValue;

use super::WhereBuilder;

pub struct SelectBuilder<'s> {
    pub select_fields: Vec<StrOrString<'static>>,
    pub where_model: Option<WhereBuilder<'s>>,
    values: Vec<SqlValue<'s>>,
}

impl<'s> SelectBuilder<'s> {
    pub fn new() -> Self {
        Self {
            select_fields: Vec::new(),
            where_model: None,
            values: Vec::new(),
        }
    }
}
