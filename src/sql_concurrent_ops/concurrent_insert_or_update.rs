use std::collections::HashMap;

use crate::{sql_insert::SqlInsertModel, sql_where::SqlWhereModel, SqlValue, UpdateConflictType};

pub fn build_concurrent_insert_or_update<
    's,
    TSqlInsertModel: SqlInsertModel<'s>,
    TSqlWhereModel: SqlWhereModel<'s>,
>(
    table_name: &str,
    insert_model: &'s TSqlInsertModel,
    where_model: &'s TSqlWhereModel,
    update_conflict_type: UpdateConflictType<'s>,
) -> (String, Vec<SqlValue<'s>>) {
    let mut params = Vec::new();
    let update_fields = HashMap::new();

    let (mut sql, update_fields) =
        crate::sql_insert::build_insert(table_name, insert_model, &mut params, Some(update_fields));

    update_conflict_type.generate_sql(&mut sql);

    sql.push_str(" DO UPDATE SET ");

    fill_update_part(&mut sql, update_fields.as_ref().unwrap());

    where_model.build_where(&mut sql, &mut params);
    where_model.fill_limit_and_offset(&mut sql);

    (sql, params)
}

fn fill_update_part(sql: &mut String, update_fields: &HashMap<&'static str, usize>) {
    todo!("")
}
