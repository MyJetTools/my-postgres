use crate::{sql_update::SqlUpdateModelValue, ColumnName};

pub trait SqlInsertModel {
    fn get_fields_amount() -> usize;
    fn get_column_name(no: usize) -> (ColumnName, Option<ColumnName>);
    fn get_field_value(&self, no: usize) -> SqlUpdateModelValue;

    fn get_e_tag_column_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);

    fn generate_insert_fields(sql: &mut String) {
        sql.push('(');
        let mut no = 0;
        for field_no in 0..Self::get_fields_amount() {
            if no > 0 {
                sql.push(',');
            }
            no += 1;
            let (field_name, additional_field_name) = Self::get_column_name(field_no);

            field_name.push_name(sql);

            if let Some(additional_field_name) = additional_field_name {
                sql.push(',');
                additional_field_name.push_name(sql);
                no += 1;
            }
        }

        sql.push(')');
    }
}
