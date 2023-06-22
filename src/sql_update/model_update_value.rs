use crate::{sql::SqlValues, SqlValueMetadata};

use super::SqlUpdateValueProvider;

pub struct SqlUpdateModelValue<'s> {
    pub metadata: Option<SqlValueMetadata>,
    pub value: Option<&'s dyn SqlUpdateValueProvider<'s>>,
}

impl<'s> SqlUpdateModelValue<'s> {
    pub fn write_value(
        &self,
        sql: &mut String,
        params: &mut SqlValues<'s>,
        get_column_name: impl Fn() -> (&'static str, Option<&'static str>),
    ) {
        match &self.value {
            Some(value) => {
                let value = value.get_update_value(params, &self.metadata);
                value.write(sql)
            }
            None => {
                let (_, related_column_name) = get_column_name();
                if related_column_name.is_none() {
                    sql.push_str("NULL");
                } else {
                    sql.push_str("NULL,NULL");
                }
            }
        }
    }
}
