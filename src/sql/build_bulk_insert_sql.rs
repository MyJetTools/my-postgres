use crate::sql_insert::SqlInsertModel;

use super::SqlValues;

pub fn build_bulk_insert_sql<'s, TSqlInsertModel: SqlInsertModel<'s>>(
    models: &'s [TSqlInsertModel],
    table_name: &str,
) -> (String, SqlValues<'s>) {
    let mut result = String::new();

    result.push_str("INSERT INTO ");
    result.push_str(table_name);

    TSqlInsertModel::generate_insert_fields(&mut result);

    result.push_str(" VALUES ");

    let mut params = SqlValues::new();

    fill_bulk_insert_values_sql::<TSqlInsertModel>(models, &mut result, &mut params);

    (result, params)
}

fn fill_bulk_insert_values_sql<'s, TSqlInsertModel: SqlInsertModel<'s>>(
    models: &'s [impl SqlInsertModel<'s>],
    sql: &mut String,
    params: &mut SqlValues<'s>,
) {
    let mut model_no = 0;
    for model in models {
        if model_no > 0 {
            sql.push(',');
        }
        model_no += 1;
        super::generate_insert_fields_values(model, sql, params);
    }
}
