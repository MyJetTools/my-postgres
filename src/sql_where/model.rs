use crate::{SqlValue, SqlValueMetadata, SqlWhereValueWriter};

pub struct WhereFieldData<'s> {
    pub field_name: &'s str,
    pub op: Option<&'s str>,
    pub value: &'s dyn SqlWhereValueWriter<'s>,
    pub meta_data: Option<SqlValueMetadata>,
}
pub trait SqlWhereModel<'s> {
    fn get_where_field_name_data(&self, no: usize) -> Option<WhereFieldData<'s>>;

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;

    fn build_where(&self, sql: &mut String, params: &mut Vec<SqlValue<'s>>) {
        let mut no = 0;

        while let Some(field_data) = self.get_where_field_name_data(no) {
            if no > 0 {
                sql.push_str(" AND ");
            } else {
                sql.push_str(" WHERE ");
            }

            no += 1;
            sql.push_str(field_data.field_name);
            if let Some(op) = field_data.op {
                sql.push_str(op);
            } else {
                sql.push('=');
            }
            field_data.value.write(sql, params, &field_data.meta_data);
        }
    }

    fn fill_limit_and_offset(&self, sql: &mut String) {
        if let Some(limit) = self.get_limit() {
            sql.push_str(" LIMIT ");
            sql.push_str(limit.to_string().as_str());
        }
        if let Some(offset) = self.get_offset() {
            sql.push_str(" OFFSET ");
            sql.push_str(offset.to_string().as_str());
        }
    }
}
