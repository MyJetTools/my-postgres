use std::collections::HashMap;

use crate::sql_where::SqlWhereModel;

use super::SqlUpdateModel;

pub fn build<'s, TSqlUpdateModel: SqlUpdateModel<'s>, TSqlWhereModel: SqlWhereModel<'s>>(
    table_name: &str,
    update_model: &'s TSqlUpdateModel,
    where_model: &'s TSqlWhereModel,
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut result = String::new();

    result.push_str("UPDATE ");
    result.push_str(table_name);
    result.push_str(" SET (");

    let mut params = Vec::new();

    build_update_part(&mut result, &mut params, update_model, None);

    result.push_str(") WHERE ");

    crate::sql_where::build(&mut result, where_model, &mut params);

    (result, params)
}

pub fn build_update_part<'s, TSqlUpdateModel: SqlUpdateModel<'s>>(
    result: &mut String,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
    update_model: &'s TSqlUpdateModel,
    cached_fields: Option<HashMap<&'static str, usize>>,
) {
    for i in 0..TSqlUpdateModel::get_fields_amount() {
        if i > 0 {
            result.push(',');
        }
        let update_data = update_model.get_field_value(i);

        result.push_str(update_data.name);
        result.push_str("=");

        if let Some(cached_fields) = &cached_fields {
            if let Some(value) = cached_fields.get(update_data.name) {
                result.push_str("$");
                result.push_str(value.to_string().as_str());
            }
        } else {
            match update_data.value {
                crate::SqlValue::Ignore => {}
                crate::SqlValue::Null => {
                    result.push_str("NULL");
                }
                crate::SqlValue::Value { options, value } => {
                    value.write(result, params, options.as_ref());
                }
            }
        }
    }
}
