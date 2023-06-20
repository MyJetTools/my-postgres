use crate::{sql_where::SqlWhereModel, SqlValue};

use super::SelectEntity;

pub fn build<'s, TSelectModel: SelectEntity, TWhereModel: SqlWhereModel<'s>>(
    table_name: &str,

    where_model: Option<&'s TWhereModel>,
    order_by_fields: Option<&str>,
    group_by_fields: Option<&str>,
) -> (String, Vec<SqlValue<'s>>) {
    let mut sql = String::new();
    let mut params = Vec::new();

    sql.push_str("SELECT ");
    TSelectModel::fill_select_fields(&mut sql);
    sql.push_str(" FROM ");
    sql.push_str(table_name);

    if let Some(where_model) = where_model {
        where_model.build_where(&mut sql, &mut params);
    }

    if let Some(order_by_fields) = order_by_fields {
        sql.push_str(order_by_fields);
    }

    if let Some(group_by_fields) = group_by_fields {
        sql.push_str(group_by_fields);
    }

    if let Some(where_model) = where_model {
        where_model.fill_limit_and_offset(&mut sql);
    }

    (sql, params)
}
