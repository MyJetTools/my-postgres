use crate::{SqlUpdateValueWriter, SqlValueMetadata, SqlWhereValueWriter};

#[derive(Debug)]
pub enum SqlValue<'s> {
    ValueAsString(String),
    ValueAsStaticStr(&'static str),
    Ref(&'s (dyn tokio_postgres::types::ToSql + Sync)),
}

impl<'s> SqlValue<'s> {
    pub fn get_value(&'s self) -> &'s (dyn tokio_postgres::types::ToSql + Sync) {
        match self {
            SqlValue::ValueAsString(value) => value,
            SqlValue::Ref(value) => *value,
            SqlValue::ValueAsStaticStr(value) => value,
        }
    }
}

pub enum SqlWhereValueWrapper<'s> {
    Ignore,
    Null,
    Value {
        metadata: Option<SqlValueMetadata>,
        value: &'s dyn SqlWhereValueWriter<'s>,
    },
}
