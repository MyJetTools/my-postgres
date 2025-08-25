use crate::{sql::UsedColumns, sql_update::SqlUpdateModelValue, ColumnName};

pub trait SqlInsertModel {
    fn get_fields_amount() -> usize;
    fn get_column_name(no: usize) -> ColumnName;
    fn get_field_value<'s>(&'s self, no: usize) -> SqlUpdateModelValue<'s>;

    fn get_e_tag_column_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);

    fn generate_insert_fields(sql: &mut String, used_columns: &UsedColumns) {
        sql.push('(');
        let mut no = 0;
        for field_no in 0..Self::get_fields_amount() {
            let column_name = Self::get_column_name(field_no);

            if used_columns.has_column(&column_name) {
                if no > 0 {
                    sql.push(',');
                }
                no += 1;
                column_name.push_name(sql);
            }
        }

        sql.push(')');
    }

    fn get_insert_columns_list(&self) -> UsedColumns {
        let fields_amount = Self::get_fields_amount();
        let mut result = Vec::with_capacity(fields_amount);
        for field_no in 0..Self::get_fields_amount() {
            let value = self.get_field_value(field_no);

            if value.ignore_if_none && value.value.is_none() {
                continue;
            }

            let field_name = Self::get_column_name(field_no);
            result.push(field_name);
        }
        result.into()
    }
}
