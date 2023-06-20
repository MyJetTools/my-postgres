use std::collections::HashMap;

use crate::{sql_update::SqlUpdateModel, SqlValue, UpdateConflictType};

use super::SqlInsertModel;

pub fn build_insert_or_update<'s, TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
    table_name: &str,
    update_conflict_type: &UpdateConflictType<'s>,
    model: &'s TSqlInsertModel,
) -> (String, Vec<SqlValue<'s>>) {
    let mut params = Vec::new();

    let update_fields = HashMap::new();
    let (mut sql, update_fields) =
        super::build_insert(table_name, model, &mut params, Some(update_fields));

    update_conflict_type.generate_sql(&mut sql);

    sql.push_str(" DO UPDATE SET ");

    model.build_update_sql(&mut sql, &mut params, update_fields.as_ref());

    (sql, params)
}
