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
        let where_model = where_models.get(0).unwrap();

        where_model.build_where(&mut sql, &mut params);
        where_model.fill_limit_and_offset(&mut sql);
    } else {
        let mut no = 0;
        for where_model in where_models {
            if no > 0 {
                sql.push_str(" OR ");
            }
            sql.push('(');
            where_model.build_where(&mut sql, &mut params);
            sql.push(')');

            where_model.fill_limit_and_offset(&mut sql);
            no += 1;
        }
    }

    (sql, params)
}
