use crate::{
    sql_insert::SqlInsertModel, sql_update::SqlUpdateModel, sql_where::SqlWhereModel, SqlValue,
    UpdateConflictType,
};

pub fn build_concurrent_insert_or_update<
    's,
    TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    TSqlWhereModel: SqlWhereModel<'s>,
>(
    table_name: &str,
    insert_model: &'s TSqlInsertModel,
    where_model: &'s TSqlWhereModel,
    update_conflict_type: UpdateConflictType<'s>,
) -> (String, Vec<SqlValue<'s>>) {
    let (mut sql, mut params) = insert_model.build_insert_sql(table_name);

    update_conflict_type.generate_sql(&mut sql);

    sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql);

    where_model.build_where_sql_part(&mut sql, &mut params, true);
    where_model.fill_limit_and_offset(&mut sql);

    (sql, params)
}
