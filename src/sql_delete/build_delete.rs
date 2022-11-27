pub fn build_delete<'s, TSqlWhereModel: crate::sql_where::SqlWhereModel<'s>>(
    table_name: &str,
    where_model: &'s TSqlWhereModel,
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut sql = String::new();

    sql.push_str("DELETE FROM ");
    sql.push_str(table_name);
    sql.push_str(" WHERE ");

    let mut params = Vec::new();
    params = where_model.fill_where(&mut sql, params);

    (sql, params)
}
