use crate::{sql_insert::SqlInsertModel, sql_update::SqlUpdateModel};

use super::SqlValues;

pub fn build_insert_or_update_sql<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    model: &TSqlInsertModel,
    table_name: &str,
    update_conflict_type: &crate::UpdateConflictType<'s>,
) -> (String, SqlValues<'s>) {
    let (mut sql, params) = super::build_insert_sql(model, table_name);

    update_conflict_type.generate_sql(&mut sql);

    sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql);

    (sql, params)
}
