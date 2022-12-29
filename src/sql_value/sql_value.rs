use crate::{SqlValueMetadata, SqlValueWriter};

#[derive(Debug)]
pub enum SqlValue<'s> {
    ValueAsString(String),
    Ref(&'s (dyn tokio_postgres::types::ToSql + Sync)),
}

impl<'s> SqlValue<'s> {
    pub fn get_value(&'s self) -> &'s (dyn tokio_postgres::types::ToSql + Sync) {
        match self {
            SqlValue::ValueAsString(value) => value,
            SqlValue::Ref(value) => *value,
        }
    }
}

pub enum SqlValueWrapper<'s> {
    Ignore,
    Null,
    Value {
        metadata: Option<SqlValueMetadata>,
        value: &'s dyn SqlValueWriter<'s>,
    },
}
