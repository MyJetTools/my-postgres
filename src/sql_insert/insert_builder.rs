use crate::SqlValue;

use super::SqlInsertModel;

pub fn build_insert<'s, TSqlInsertModel: SqlInsertModel<'s>>(
    table_name: &str,
    insert_model: &'s TSqlInsertModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
) -> String {
    let mut result = String::new();

    result.push_str("INSERT ");
    result.push_str(table_name);
    result.push_str(" (");

    let mut no = 0;

    let mut values = Vec::new();

    for field_no in 0..TSqlInsertModel::get_fields_amount() {
        let sql_value = insert_model.get_field_value(field_no);

        match sql_value {
            SqlValue::Ignore => {}
            SqlValue::Value(value) => {
                if no > 0 {
                    result.push(',');
                }
                no += 1;
                result.push_str(TSqlInsertModel::get_field_name(field_no));
                values.push(value);
            }
        }
    }

    for value in values {
        if let Some(value) = value {
            value.write(&mut result, params);
        } else {
            result.push_str("NULL");
        }
    }

    result
}
