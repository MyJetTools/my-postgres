use crate::{sql_insert::SqlInsertModel, sql_update::SqlUpdateModel};

use super::SqlValues;

pub fn build_upsert_sql<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    model: &TSqlInsertModel,
    table_name: &str,
    update_conflict_type: &crate::UpdateConflictType<'s>,
    e_tag_value: i64,
) -> (String, SqlValues<'s>) {
    if TSqlInsertModel::get_e_tag_column_name().is_some() {
        model.set_e_tag_value(e_tag_value);
    }

    let (mut sql, params) = crate::sql::build_insert_sql(model, table_name);

    update_conflict_type.generate_sql(&mut sql);

    sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql);

    fill_upsert_where_condition(model, &mut sql);

    (sql, params)
}

fn fill_upsert_where_condition<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    model: &TSqlInsertModel,
    sql: &mut String,
) {
    if let Some(e_tag_column) = TSqlInsertModel::get_e_tag_column_name() {
        if let Some(value) = model.get_e_tag_value() {
            sql.push_str(" WHERE EXCLUDED.");
            sql.push_str(e_tag_column);
            sql.push('=');

            sql.push_str(value.to_string().as_str());
        }
    }
}
