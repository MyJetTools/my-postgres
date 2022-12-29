use crate::SqlValue;

pub fn build_bulk_delete<'s, TSqlWhereModel: crate::sql_where::SqlWhereModel<'s>>(
    table_name: &str,
    where_models: &'s [TSqlWhereModel],
) -> (String, Vec<SqlValue<'s>>) {
    let mut sql = String::new();

    sql.push_str("DELETE FROM ");
    sql.push_str(table_name);
    sql.push_str(" WHERE ");

    let mut params = Vec::new();

    if where_models.len() == 1 {
        where_models
            .get(0)
            .unwrap()
            .fill_where(&mut sql, &mut params);
    } else {
        let mut no = 0;
        for where_model in where_models {
            if no > 0 {
                sql.push_str(" OR ");
            }
            sql.push('(');
            where_model.fill_where(&mut sql, &mut params);
            sql.push(')');
            no += 1;
        }
    }

    (sql, params)
}
