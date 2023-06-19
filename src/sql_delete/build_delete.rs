use crate::SqlValue;

pub fn build_delete<'s, TSqlWhereModel: crate::sql_where::SqlWhereModel<'s>>(
    table_name: &str,
    where_model: &'s TSqlWhereModel,
) -> (String, Vec<SqlValue<'s>>) {
    let mut sql = String::new();

    sql.push_str("DELETE FROM ");
    sql.push_str(table_name);

    let mut params = Vec::new();
    where_model.build_where(&mut sql, &mut params);
    where_model.fill_limit_and_offset(&mut sql);
    (sql, params)
}
