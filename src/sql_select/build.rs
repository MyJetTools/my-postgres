use crate::{sql_where::SqlWhereModel, SqlValue};

pub fn build<'s, TFillSelect: Fn(&mut String), TWhereModel: SqlWhereModel<'s>>(
    table_name: &str,
    fill_select: TFillSelect,
    where_model: Option<&'s TWhereModel>,
    order_by_fields: Option<&str>,
    group_by_fields: Option<&str>,
) -> (String, Vec<SqlValue<'s>>) {
    let mut sql = String::new();
    let mut params = Vec::new();

    sql.push_str("SELECT ");
    fill_select(&mut sql);
    sql.push_str(" FROM ");
    sql.push_str(table_name);

    if let Some(where_model) = where_model {
        sql.push_str(" WHERE ");
        where_model.fill_where(&mut sql, &mut params);
    }

    if let Some(order_by_fields) = order_by_fields {
        sql.push_str(order_by_fields);
    }

    if let Some(group_by_fields) = group_by_fields {
        sql.push_str(group_by_fields);
    }

    if let Some(where_model) = where_model {
        if let Some(limit) = where_model.get_limit() {
            sql.push_str(" LIMIT ");
            sql.push_str(limit.to_string().as_str());
        }

        if let Some(offset) = where_model.get_offset() {
            sql.push_str(" OFFSET ");
            sql.push_str(offset.to_string().as_str());
        }
    }

    (sql, params)
}
