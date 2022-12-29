use crate::{sql_update::SqlUpdateModel, SqlValue};

use super::SqlInsertModel;

pub fn build_bulk_insert_if_update<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    table_name: &str,
    primary_key: &str,
    models: &'s [TSqlInsertModel],
) -> Vec<(String, Vec<SqlValue<'s>>)> {
    let mut sqls = Vec::new();

    for model in models {
        sqls.push(super::build_insert_or_update(
            table_name,
            primary_key,
            model,
        ));
    }

    sqls
}
