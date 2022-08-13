use super::super::{NumberedParams, SqlValue, WhereBuilder};

const AND_OPERATOR: &str = "AND";

pub struct DeleteInner<'s> {
    pub where_builder: WhereBuilder<'s>,
    pub has_value: bool,
}

impl<'s> DeleteInner<'s> {
    pub fn new() -> Self {
        Self {
            where_builder: WhereBuilder::new(AND_OPERATOR),
            has_value: false,
        }
    }

    pub fn add_where_field(
        &mut self,
        numbered_params: &mut NumberedParams<'s>,
        field_name: &str,
        sql_value: SqlValue<'s>,
    ) {
        let sql_value = numbered_params.add_or_get(sql_value);
        self.where_builder.add(field_name, sql_value);
        self.has_value = true;
    }

    pub fn build_where(&self, dest: &mut String) {
        self.where_builder.build(dest);
    }

    pub fn build(&mut self, table_name: &str, dest: &mut String) {
        dest.push_str("DELETE FROM ");
        dest.push_str(table_name);
        dest.push_str(" WHERE ");
        self.build_where(dest);
    }
}
