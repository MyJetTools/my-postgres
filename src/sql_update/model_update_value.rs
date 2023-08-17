use crate::{sql::SqlValues, SqlValueMetadata};

use super::SqlUpdateValueProvider;

pub struct SqlUpdateModelValue<'s> {
    pub metadata: Option<SqlValueMetadata>,
    pub ignore_if_none: bool,
    pub value: Option<&'s dyn SqlUpdateValueProvider>,
}

impl<'s> SqlUpdateModelValue<'s> {
    pub fn write_value(&self, sql: &mut String, params: &mut SqlValues) {
        match &self.value {
            Some(value) => {
                let value = value.get_update_value(params, &self.metadata);
                value.write(sql)
            }
            None => {
                sql.push_str("NULL");
            }
        }
    }
}
