use crate::{sql_insert::SqlInsertModel, sql_update::SqlUpdateModel};

use super::{SqlData, UsedColumns};

pub fn build_insert_or_update_sql<'s, TSqlInsertModel: SqlInsertModel + SqlUpdateModel>(
    model: &TSqlInsertModel,
    table_name: &str,
    update_conflict_type: &crate::UpdateConflictType<'s>,
) -> SqlData {
    let mut columns = UsedColumns::new_as_active();
    let mut sql_data = super::build_insert_sql(model, table_name, &mut columns);

    update_conflict_type.generate_sql(&mut sql_data.sql);

    sql_data.sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql_data.sql, &columns.into());

    sql_data
}
