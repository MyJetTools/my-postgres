use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{sql_update::SqlUpdateModel, SqlValue};

use super::SqlInsertModel;

pub fn build_bulk_insert_if_update<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    table_name: &str,
    primary_key: &str,
    insert_or_update_models: &'s [TSqlInsertModel],
) -> Vec<(String, Vec<SqlValue<'s>>)> {
    let mut sql = Vec::new();

    let has_e_tag = TSqlInsertModel::get_e_tag_insert_field_name().is_some();

    for model in insert_or_update_models {
        if has_e_tag {
            let value = DateTimeAsMicroseconds::now();
            model.set_e_tag_insert_value(value.unix_microseconds);
        }

        sql.push(super::build_insert_or_update(
            table_name,
            primary_key,
            model,
        ));
    }

    sql
}
