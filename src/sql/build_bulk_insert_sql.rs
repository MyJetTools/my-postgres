use crate::sql_insert::SqlInsertModel;

use super::{SqlData, SqlValues};

pub fn build_bulk_insert_sql<TSqlInsertModel: SqlInsertModel>(
    models: &[TSqlInsertModel],
    table_name: &str,
) -> SqlData {
    let mut result = String::new();

    result.push_str("INSERT INTO ");
    result.push_str(table_name);

    TSqlInsertModel::generate_insert_fields(&mut result);

    result.push_str(" VALUES ");

    let mut params = SqlValues::new();

    fill_bulk_insert_values_sql::<TSqlInsertModel>(models, &mut result, &mut params);

    SqlData::new(result, params)
}

fn fill_bulk_insert_values_sql<TSqlInsertModel: SqlInsertModel>(
    models: &[impl SqlInsertModel],
    sql: &mut String,
    params: &mut SqlValues,
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
