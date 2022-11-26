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

    if let Some(order_by_fields) = TWhereModel::get_order_by_fields() {
        match order_by_fields {
            crate::sql_where::OrderByFields::Asc(fields) => {
                sql.push_str(" ORDER BY ");

                for (no, field) in fields.into_iter().enumerate() {
                    if no > 0 {
                        sql.push_str(",");
                    }

                    sql.push_str(field);
                }
            }
            crate::sql_where::OrderByFields::Desc(fields) => {
                sql.push_str(" ORDER BY ");

                for (no, field) in fields.into_iter().enumerate() {
                    if no > 0 {
                        sql.push_str(",");
                    }

                    sql.push_str(field);
                }

                sql.push_str(" DESC");
            }
        }
    }

    crate::sql_where::build(&mut sql, model, &mut params);

    (sql, params)
}
