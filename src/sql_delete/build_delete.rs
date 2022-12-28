use crate::SqlValueToWrite;

pub fn build_delete<'s, TSqlWhereModel: crate::sql_where::SqlWhereModel<'s>>(
    table_name: &str,
    where_model: &'s TSqlWhereModel,
) -> (String, Vec<SqlValueToWrite<'s>>) {
    let mut sql = String::new();

    sql.push_str("DELETE FROM ");
    sql.push_str(table_name);
    sql.push_str(" WHERE ");

    let mut params = Vec::new();
    where_model.fill_where(&mut sql, &mut params);

    (sql, params)
}
