pub fn build_bulk_delete<'s, TSqlWhereModel: crate::sql_where::SqlWhereModel<'s>>(
    table_name: &str,
    where_models: &'s [TSqlWhereModel],
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut sql = String::new();

    sql.push_str("DELETE FROM ");
    sql.push_str(table_name);
    sql.push_str(" WHERE ");

    let mut params = Vec::new();

    let no = 0;
    for where_model in where_models {
        if no > 0 {
            sql.push_str(" OR ");
        }
        where_model.fill_where(&mut sql, &mut params);
    }

    (sql, params)
}
