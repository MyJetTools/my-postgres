use std::collections::HashMap;

use crate::sql_update::SqlUpdateModel;

use super::SqlInsertModel;

pub fn build_insert_or_update<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    table_name: &str,
    primary_key_name: &str,
    model: &'s TSqlInsertModel,
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut params = Vec::new();

    let update_fields = HashMap::new();
    let (mut sql, update_fields) =
        super::build_insert(table_name, model, &mut params, Some(update_fields));

    sql.push_str(" ON CONFLICT ON CONSTRAINT ");

    sql.push_str(primary_key_name);

    sql.push_str(" DO UPDATE (");

    crate::sql_update::build_update_part(&mut sql, &mut params, model, update_fields);

    sql.push_str(" )");
    (sql, params)
}
