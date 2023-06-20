use crate::{sql_where::SqlWhereModel, SqlValue};

use super::SqlUpdateModel;

pub fn build<'s, TSqlUpdateModel: SqlUpdateModel<'s>, TSqlWhereModel: SqlWhereModel<'s>>(
    table_name: &str,
    update_model: &'s TSqlUpdateModel,
    where_model: &'s TSqlWhereModel,
) -> (String, Vec<SqlValue<'s>>) {
    let mut result = String::new();

    result.push_str("UPDATE ");
    result.push_str(table_name);
    result.push_str(" SET ");

    let mut params = Vec::new();

    update_model.build_update_sql(&mut result, &mut params, None);

    where_model.build_where(&mut result, &mut params);

    where_model.fill_limit_and_offset(&mut result);

    //crate::sql_where::build(&mut result, where_model, &mut params);

    (result, params)
}
