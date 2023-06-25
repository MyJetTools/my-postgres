use crate::sql_update::SqlUpdateModelValue;

pub trait SqlInsertModel<'s> {
    fn get_fields_amount() -> usize;
    fn get_column_name(no: usize) -> (&'static str, Option<&'static str>);
    fn get_field_value(&self, no: usize) -> SqlUpdateModelValue<'s>;

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
            sql.push_str(field_name);
            if let Some(additional_field_name) = additional_field_name {
                sql.push(',');
                sql.push_str(additional_field_name);
                no += 1;
            }
        }

        sql.push(')');
    }
}
