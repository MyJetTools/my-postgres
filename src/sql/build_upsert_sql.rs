use crate::{sql_insert::SqlInsertModel, sql_update::SqlUpdateModel};

use super::{SqlData, UpsertColumns};

pub fn build_upsert_sql<TSqlInsertModel: SqlInsertModel + SqlUpdateModel>(
    model: &TSqlInsertModel,
    table_name: &str,
    update_conflict_type: &crate::UpdateConflictType,
    e_tag_value: i64,
) -> SqlData {
    let old_e_tag = model.get_e_tag_value().unwrap();
    if TSqlInsertModel::get_e_tag_column_name().is_some() {
        model.set_e_tag_value(e_tag_value);
    }

    let mut columns = UpsertColumns::new_as_active();

    let mut sql_data = crate::sql::build_insert_sql(model, table_name, &mut columns);

    update_conflict_type.generate_sql(&mut sql_data.sql);

    sql_data.sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql_data.sql, columns.as_slice());

    fill_upsert_where_condition::<TSqlInsertModel>(&mut sql_data.sql, old_e_tag);

    sql_data
}

fn fill_upsert_where_condition<TSqlInsertModel: SqlInsertModel + SqlUpdateModel>(
    sql: &mut String,
    e_tag_value: i64,
) {
    if let Some(e_tag_column) = TSqlInsertModel::get_e_tag_column_name() {
        sql.push_str(" WHERE EXCLUDED.");
        sql.push_str(e_tag_column);
        sql.push('=');

        sql.push_str(e_tag_value.to_string().as_str());
    }
}
