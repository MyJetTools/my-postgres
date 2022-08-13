use super::super::{NumberedParams, SqlValue};

use super::DeleteInner;

pub struct DeleteBuilder<'s> {
    numbered_params: NumberedParams<'s>,
    inner: DeleteInner<'s>,
}

impl<'s> DeleteBuilder<'s> {
    pub fn new() -> Self {
        Self {
            inner: DeleteInner::new(),
            numbered_params: NumberedParams::new(),
        }
    }

    pub fn add_where_field(&'s mut self, field_name: &str, sql_value: SqlValue<'s>) {
        self.inner
            .add_where_field(&mut self.numbered_params, field_name, sql_value);
    }

    pub fn build(&'s mut self, table_name: &str) -> String {
        let mut result = String::new();
        self.inner.build(table_name, &mut result);
        result
    }

    pub fn get_values_data(&mut self) -> &'s [&(dyn tokio_postgres::types::ToSql + Sync)] {
        self.numbered_params.build_params()
    }
}

impl<'s> super::DeleteCodeGen<'s> for DeleteBuilder<'s> {
    fn add_where_field(&mut self, field_name: &str, sql_value: SqlValue<'s>) {
        self.inner
            .add_where_field(&mut self.numbered_params, field_name, sql_value);
    }
}
