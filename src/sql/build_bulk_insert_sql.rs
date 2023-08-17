use crate::sql_insert::SqlInsertModel;

use super::{SqlData, SqlValues, UsedColumns};

pub fn build_bulk_insert_sql<TSqlInsertModel: SqlInsertModel>(
    models: &[TSqlInsertModel],
    table_name: &str,
    used_columns: &UsedColumns,
) -> SqlData {
    if models.is_empty() {
        panic!("No models to insert");
    }

    let mut result = String::new();

    result.push_str("INSERT INTO ");
    result.push_str(table_name);

    TSqlInsertModel::generate_insert_fields(&mut result, used_columns);

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
        generate_insert_fields_values(model, sql, params);
    }
}

fn generate_insert_fields_values<TInsertSql: SqlInsertModel>(
    model: &TInsertSql,
    sql: &mut String,
    params: &mut SqlValues,
) {
    sql.push('(');

    let mut field_no_rendered = 0;
    for field_no in 0..TInsertSql::get_fields_amount() {
        let update_value = model.get_field_value(field_no);

        if field_no_rendered > 0 {
            sql.push(',');
        }

        field_no_rendered += 1;

        update_value.write_value(sql, params);
    }
    sql.push(')');
}
