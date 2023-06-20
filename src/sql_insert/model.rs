use crate::SqlUpdateValueWrapper;

pub trait SqlInsertModel<'s> {
    fn get_fields_amount() -> usize;
    fn get_field_name(no: usize) -> &'static str;
    fn get_field_value(&'s self, no: usize) -> SqlUpdateValueWrapper<'s>;

    fn get_e_tag_column_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);

    fn generate_insert_fields(
        &'s self,
        sql: &mut String,
        params: &mut Vec<crate::SqlValue<'s>>,
        params_with_index: &mut Option<std::collections::HashMap<&'static str, usize>>,
    ) {
        sql.push_str(" (");

        let mut no = 0;

        let mut values = Vec::new();

        for field_no in 0..Self::get_fields_amount() {
            let sql_value = self.get_field_value(field_no);

            match &sql_value {
                SqlUpdateValueWrapper::Ignore => {}
                SqlUpdateValueWrapper::Value {
                    value: _,
                    metadata: _,
                } => {
                    if no > 0 {
                        sql.push(',');
                    }
                    no += 1;
                    let field_name = Self::get_field_name(field_no);
                    sql.push_str(field_name);
                    values.push((field_name, sql_value));
                }
                SqlUpdateValueWrapper::Null => {}
            }
        }
        sql.push_str(") VALUES (");
        no = 0;
        for (field_name, sql_value) in values {
            match sql_value {
                SqlUpdateValueWrapper::Ignore => {}
                SqlUpdateValueWrapper::Null => {
                    if no > 0 {
                        sql.push(',');
                    }
                    no += 1;

                    sql.push_str("NULL");
                }
                SqlUpdateValueWrapper::Value { metadata, value } => {
                    if no > 0 {
                        sql.push(',');
                    }
                    no += 1;

                    let pos = sql.len();
                    value.write(sql, params, &metadata);

                    if let Some(params_with_index) = params_with_index {
                        let param = &sql[pos..];

                        if param.starts_with('$') {
                            params_with_index.insert(field_name, params.len());
                        }
                    }
                }
            }
        }

        sql.push(')');
    }
}
