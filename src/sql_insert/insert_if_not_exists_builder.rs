use super::SqlInsertModel;

pub fn build_insert_if_not_exists<'s, TSqlInsertModel: SqlInsertModel<'s>>(
    table_name: &str,
    insert_model: &'s TSqlInsertModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
) -> String {
    let mut sql = super::build_insert(table_name, insert_model, params);
    sql.push_str(" ON CONFLICT DO NOTHING");
    sql
}
