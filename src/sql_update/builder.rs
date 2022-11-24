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

    build_update_part(&mut result, &mut params, update_model);

    result.push_str(") WHERE ");

    crate::sql_where::build(&mut result, where_model, &mut params);

    (result, params)
}

pub fn build_update_part<'s, TSqlUpdateModel: SqlUpdateModel<'s>>(
    result: &mut String,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
    update_model: &'s TSqlUpdateModel,
) {
    for i in 0..TSqlUpdateModel::get_fields_amount() {
        if i > 0 {
            result.push(',');
        }
        let update_data = update_model.get_field_value(i);

        result.push_str(update_data.name);
        result.push_str("=");

        if let Some(value) = update_data.value {
            value.write(result, params);
        } else {
            result.push_str("NULL");
        }
    }
}
