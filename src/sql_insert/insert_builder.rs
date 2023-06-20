use std::collections::HashMap;

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::SqlValue;

use super::SqlInsertModel;

pub fn build_insert<'s, TSqlInsertModel: SqlInsertModel<'s>>(
    table_name: &str,
    insert_model: &'s TSqlInsertModel,
    params: &mut Vec<SqlValue<'s>>,
    mut params_with_index: Option<HashMap<&'static str, usize>>,
) -> (String, Option<HashMap<&'static str, usize>>) {
    if TSqlInsertModel::get_e_tag_column_name().is_some() {
        let value = DateTimeAsMicroseconds::now();
        insert_model.set_e_tag_value(value.unix_microseconds);
    }

    let mut result = String::new();

    result.push_str("INSERT INTO ");
    result.push_str(table_name);

    TSqlInsertModel::generate_insert_fields(
        insert_model,
        &mut result,
        params,
        &mut params_with_index,
    );

    (result, params_with_index)
}
