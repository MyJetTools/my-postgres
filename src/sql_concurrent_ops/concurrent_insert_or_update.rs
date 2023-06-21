use crate::{
    sql::SqlValues, sql_insert::SqlInsertModel, sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel, UpdateConflictType,
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
) -> (String, SqlValues<'s>) {
    let (mut sql, mut params) = insert_model.build_insert_sql(table_name);

    update_conflict_type.generate_sql(&mut sql);

    sql.push_str(" DO UPDATE SET ");

    TSqlInsertModel::fill_upsert_sql_part(&mut sql);

    let where_builder = where_model.build_where_sql_part(&mut params);
    if where_builder.has_conditions() {
        sql.push_str(" WHERE ");
        where_builder.build(&mut sql);
    }

    where_model.fill_limit_and_offset(&mut sql);

    (sql, params)
}
