use crate::sql_update::SqlUpdateModel;

use super::SqlInsertModel;

pub fn build_insert_or_update<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    table_name: &str,
    primary_key_name: &str,
    model: &'s TSqlInsertModel,
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut params = Vec::new();
    let mut sql = super::build_insert(table_name, model, &mut params);

    sql.push_str(" ON CONFLICT ON CONSTRAINT (");

    sql.push_str(primary_key_name);

    sql.push_str(") DO UPDATE (");

    crate::sql_update::build_update_part(&mut sql, &mut params, model);

    sql.push_str(" )");
    (sql, params)
}
