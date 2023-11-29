use crate::{sql_update::SqlUpdateModel, sql_where::SqlWhereModel};

use super::{SqlData, SqlValues};

pub fn build_update_sql<TModel: SqlUpdateModel + SqlWhereModel>(
    model: &TModel,
    table_name: &str,
) -> SqlData {
    let mut sql = String::new();

    sql.push_str("UPDATE ");
    sql.push_str(table_name);
    sql.push_str(" SET ");

    let mut params = SqlValues::new();

    model.build_update_sql_part(&mut sql, &mut params);

    if model.has_conditions() {
        sql.push_str(" WHERE ");
        model.fill_where_component(&mut sql, &mut params);
    }

    model.fill_limit_and_offset(&mut sql);

    SqlData::new(sql, params)
}
