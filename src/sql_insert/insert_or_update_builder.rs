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

    crate::sql_update::build_update_part(&mut sql, &mut params, model, update_fields);

    (sql, params)
}
