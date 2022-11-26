use std::collections::HashMap;

use crate::SqlValue;

use super::SqlInsertModel;

pub fn build_insert<'s, TSqlInsertModel: SqlInsertModel<'s>>(
    table_name: &str,
    insert_model: &'s TSqlInsertModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
    mut params_with_index: Option<HashMap<&'static str, usize>>,
) -> (String, Option<HashMap<&'static str, usize>>) {
    let mut result = String::new();

    result.push_str("INSERT INTO ");
    result.push_str(table_name);
    result.push_str(" (");

    let mut no = 0;

    let mut values = Vec::new();

    for field_no in 0..TSqlInsertModel::get_fields_amount() {
        let sql_value = insert_model.get_field_value(field_no);

        match &sql_value {
            SqlValue::Ignore => {}
            SqlValue::Value {
                value: _,
                sql_type: _,
            } => {
                if no > 0 {
                    result.push(',');
                }
                no += 1;
                let field_name = TSqlInsertModel::get_field_name(field_no);
                result.push_str(field_name);
                values.push((field_name, sql_value));
            }
            SqlValue::Null => {}
        }
    }
    result.push_str(") VALUES (");
    no = 0;
    for (field_name, sql_value) in values {
        match sql_value {
            SqlValue::Ignore => {}
            SqlValue::Null => {
                if no > 0 {
                    result.push(',');
                }
                no += 1;

                result.push_str("NULL");
            }
            SqlValue::Value { sql_type, value } => {
                if no > 0 {
                    result.push(',');
                }
                no += 1;

                let pos = result.len();
                value.write(&mut result, params, sql_type);

                if let Some(prms) = &mut params_with_index {
                    let param = &result[pos..];

                    if param.starts_with('$') {
                        prms.insert(field_name, params.len());
                    }
                }
            }
        }
    }

    result.push(')');

    (result, params_with_index)
}
