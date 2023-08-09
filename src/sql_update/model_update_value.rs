use crate::{sql::SqlValues, ColumnName, SqlValueMetadata};

use super::SqlUpdateValueProvider;

pub struct SqlUpdateModelValue<'s> {
    pub metadata: Option<SqlValueMetadata>,
    pub ignore_if_none: bool,
    pub value: Option<&'s dyn SqlUpdateValueProvider>,
}

impl<'s> SqlUpdateModelValue<'s> {
    pub fn write_value(
        &self,
        sql: &mut String,
        params: &mut SqlValues,
        get_column_name: impl Fn() -> (ColumnName, Option<ColumnName>),
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
