use crate::sql_where::SqlWhereModel;

use super::{GroupByFields, OrderByFields};

pub fn build<'s, TWhereModel: SqlWhereModel<'s>>(
    table_name: &str,
    sql_fields: &str,
    where_model: &'s TWhereModel,
    order_by_feilds: Option<OrderByFields>,
    group_by_fields: Option<GroupByFields>,
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut sql = String::new();
    let mut params = Vec::new();

    sql.push_str("SELECT ");
    sql.push_str(sql_fields);
    sql.push_str(" FROM ");
    sql.push_str(table_name);
    sql.push_str(" WHERE ");

    crate::sql_where::build(&mut sql, where_model, &mut params);

    if let Some(order_by_fields) = order_by_feilds {
        order_by_fields.fill_sql(&mut sql);
    }

    if let Some(group_by_fields) = group_by_fields {
        group_by_fields.fill_sql(&mut sql);
    }

    if let Some(limit) = where_model.get_limit() {
        sql.push_str(" LIMIT ");
        sql.push_str(limit.to_string().as_str());
    }

    if let Some(offset) = where_model.get_offset() {
        sql.push_str(" OFFSET ");
        sql.push_str(offset.to_string().as_str());
    }

    (sql, params)
}
