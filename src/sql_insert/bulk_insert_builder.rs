use crate::{SqlValue, SqlValueToWrite};

use super::SqlInsertModel;

pub fn build_bulk_insert<'s, TInsertModel: SqlInsertModel<'s>>(
    table_name: &str,
    models: &'s [TInsertModel],
) -> (String, Vec<SqlValueToWrite<'s>>) {
    let mut result = String::new();

    result.push_str("INSERT INTO ");
    result.push_str(table_name);
    result.push_str(" (");

    let fields_amount = TInsertModel::get_fields_amount();

    for no in 0..fields_amount {
        if no > 0 {
            result.push(',');
        }
        result.push_str(TInsertModel::get_field_name(no));
    }

    result.push_str(") VALUES ");
    let mut model_no = 0;
    let mut params = Vec::new();
    for model in models {
        if model_no > 0 {
            result.push(',');
        }
        model_no += 1;
        result.push('(');

        let mut written_no = 0;

        for no in 0..fields_amount {
            match model.get_field_value(no) {
                SqlValue::Ignore => {}
                SqlValue::Value { value, sql_type } => {
                    if written_no > 0 {
                        result.push(',');
                    }

                    written_no += 1;
                    value.write(&mut result, &mut params, sql_type);
                }
                SqlValue::Null => {
                    if written_no > 0 {
                        result.push(',');
                    }

                    written_no += 1;
                    result.push_str("NULL");
                }
            }
        }

        result.push(')');
    }

    (result, params)
}
