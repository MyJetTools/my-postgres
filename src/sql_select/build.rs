use crate::sql_where::SqlWhereModel;

pub fn build<'s, TWhereModel: SqlWhereModel<'s>>(
    table_name: &str,
    sql_fields: &str,
    model: &'s TWhereModel,
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut sql = String::new();
    let mut params = Vec::new();

    sql.push_str("SELECT ");
    sql.push_str(sql_fields);
    sql.push_str(" FROM ");
    sql.push_str(table_name);
    sql.push_str(" WHERE ");

    crate::sql_where::build(&mut sql, model, &mut params);

    (sql, params)
}
