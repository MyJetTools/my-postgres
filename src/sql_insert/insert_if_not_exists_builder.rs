use crate::SqlValueToWrite;

use super::SqlInsertModel;

pub fn build_insert_if_not_exists<'s, TSqlInsertModel: SqlInsertModel<'s>>(
    table_name: &str,
    insert_model: &'s TSqlInsertModel,
    params: &mut Vec<SqlValueToWrite<'s>>,
) -> String {
    let (mut sql, _) = super::build_insert(table_name, insert_model, params, None);
    sql.push_str(" ON CONFLICT DO NOTHING");
    sql
}
