use crate::{sql_update::SqlUpdateModel, sql_where::SqlWhereModel};

use super::SqlValues;

pub fn build_update_sql<'s, TModel: SqlUpdateModel<'s> + SqlWhereModel<'s>>(
    model: &TModel,
    table_name: &str,
) -> (String, SqlValues<'s>) {
    let mut result = String::new();

    result.push_str("UPDATE ");
    result.push_str(table_name);
    result.push_str(" SET ");

    let mut params = SqlValues::new();

    model.build_update_sql_part(&mut result, &mut params);

    let where_builder = model.build_where_sql_part(&mut params);

    if where_builder.has_conditions() {
        result.push_str(" WHERE ");
        where_builder.build(&mut result);
    }

    model.fill_limit_and_offset(&mut result);

    (result, params)
}
