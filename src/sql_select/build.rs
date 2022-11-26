use crate::sql_where::SqlWhereModel;

pub fn build<
    's,
    TFillSelect: Fn(&mut String),
    TFillOrderBy: Fn(&mut String),
    TFillGroupBy: Fn(&mut String),
    TWhereModel: SqlWhereModel<'s>,
>(
    table_name: &str,
    fill_select: TFillSelect,
    where_model: &'s TWhereModel,
    order_by_feilds: TFillOrderBy,
    group_by_fields: TFillGroupBy,
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut sql = String::new();
    let mut params = Vec::new();

    sql.push_str("SELECT ");
    fill_select(&mut sql);
    sql.push_str(" FROM ");
    sql.push_str(table_name);
    sql.push_str(" WHERE ");

    crate::sql_where::build(&mut sql, where_model, &mut params);

    order_by_feilds(&mut sql);

    group_by_fields(&mut sql);

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
