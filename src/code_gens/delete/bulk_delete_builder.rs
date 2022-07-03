use super::super::{NumberedParams, SqlValue};

use super::DeleteInner;

pub struct BulkDeleteBuilder<'s> {
    numbered_params: NumberedParams<'s>,
    inners: Vec<DeleteInner>,
    current: DeleteInner,
}

impl<'s> BulkDeleteBuilder<'s> {
    pub fn new() -> Self {
        Self {
            current: DeleteInner::new(),
            numbered_params: NumberedParams::new(),
            inners: Vec::new(),
        }
    }

    pub fn add_new_line(&mut self) {
        let old_value = std::mem::replace(&mut self.current, DeleteInner::new());
        if old_value.has_value {
            self.inners.push(old_value);
        }
    }

    pub fn add_where_field(&mut self, field_name: &str, sql_value: SqlValue) {
        self.current
            .add_where_field(&mut self.numbered_params, field_name, sql_value);
    }

    pub fn build(&mut self, table_name: &str) -> String {
        let mut result = String::new();

        let mut no = 0;

        result.push_str("DELETE FROM ");
        result.push_str(table_name);

        result.push_str(" WHERE ");

        for inner in &mut self.inners {
            push_where(&mut result, no, inner);
            no += 1;
        }

        if self.current.has_value {
            push_where(&mut result, no, &self.current);
        }
        result
    }

    pub fn get_values_data(&'s mut self) -> &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)] {
        self.numbered_params.build_params()
    }
}

fn push_where(dest: &mut String, no: usize, delete_inner: &DeleteInner) {
    if no > 0 {
        dest.push_str(" OR ");
    }
    dest.push('(');
    delete_inner.build_where(dest);
    dest.push(')');
}

impl<'s> super::DeleteCodeGen for BulkDeleteBuilder<'s> {
    fn add_where_field(&mut self, field_name: &str, sql_value: SqlValue) {
        self.current
            .add_where_field(&mut self.numbered_params, field_name, sql_value);
    }
}
