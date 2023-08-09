use crate::{sql_insert::SqlInsertModel, sql_update::SqlUpdateModel};

use super::SqlData;

pub fn build_bulk_insert_or_update_sql<TSqlInsertModel: SqlInsertModel + SqlUpdateModel>(
    table_name: &str,
    update_conflict_type: &crate::UpdateConflictType,
    insert_or_update_models: &[TSqlInsertModel],
) -> SqlData {
    if insert_or_update_models.len() == 0 {
        panic!("No models to insert");
    }

    let used_columns = insert_or_update_models[0].get_insert_columns_list();

    let mut sql_data =
        super::build_bulk_insert_sql(insert_or_update_models, table_name, &used_columns);

    update_conflict_type.generate_sql(&mut sql_data.sql);

    sql_data.sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql_data.sql, &used_columns);

    sql_data
}
