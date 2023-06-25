use crate::{sql_insert::SqlInsertModel, sql_update::SqlUpdateModel};

use super::SqlData;

pub fn build_upsert_sql<TSqlInsertModel: SqlInsertModel + SqlUpdateModel>(
    model: &TSqlInsertModel,
    table_name: &str,
    update_conflict_type: &crate::UpdateConflictType,
    e_tag_value: i64,
) -> SqlData {
    if TSqlInsertModel::get_e_tag_column_name().is_some() {
        model.set_e_tag_value(e_tag_value);
    }

    let mut sql_data = crate::sql::build_insert_sql(model, table_name);

    update_conflict_type.generate_sql(&mut sql_data.sql);

    sql_data.sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql_data.sql);

    fill_upsert_where_condition(model, &mut sql_data.sql, e_tag_value);

    sql_data
}

fn fill_upsert_where_condition<TSqlInsertModel: SqlInsertModel + SqlUpdateModel>(
    model: &TSqlInsertModel,
    sql: &mut String,
    e_tag_value: i64,
) {
    if let Some(e_tag_column) = TSqlInsertModel::get_e_tag_column_name() {
        if let Some(value) = model.get_e_tag_value() {
            sql.push_str(" WHERE EXCLUDED.");
            sql.push_str(e_tag_column);
            sql.push('=');

            sql.push_str(e_tag_value.to_string().as_str());
        }
    }
}
