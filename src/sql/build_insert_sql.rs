use crate::sql_insert::SqlInsertModel;

use super::{SqlData, SqlValues, UsedColumns};

pub fn build_insert_sql<TInsertSql: SqlInsertModel>(
    model: &TInsertSql,
    table_name: &str,
    upsert_columns_to_fill: &mut UsedColumns,
) -> SqlData {
    let mut sql = String::new();

    let mut values = SqlValues::new();

    sql.push_str("INSERT INTO ");
    sql.push_str(table_name);

    let used_columns = model.get_insert_columns_list();

    TInsertSql::generate_insert_fields(&mut sql, &used_columns);
    sql.push_str(" VALUES ");
    generate_insert_fields_values(model, &mut sql, &mut values, upsert_columns_to_fill);

    SqlData { sql, values }
}

pub fn build_insert_sql_owned<TInsertSql: SqlInsertModel>(
    model: TInsertSql,
    table_name: &str,
) -> SqlData {
    let mut sql = String::new();

    let mut values = SqlValues::new();

    sql.push_str("INSERT INTO ");
    sql.push_str(table_name);

    let used_columns = model.get_insert_columns_list();
    TInsertSql::generate_insert_fields(&mut sql, &used_columns);
    sql.push_str(" VALUES ");
    generate_insert_fields_values(&model, &mut sql, &mut values, &mut UsedColumns::as_none());

    SqlData { sql, values }
}

fn generate_insert_fields_values<TInsertSql: SqlInsertModel>(
    model: &TInsertSql,
    sql: &mut String,
    params: &mut SqlValues,
    upsert_columns_to_fill: &mut UsedColumns,
) {
    sql.push('(');

    let mut field_no_rendered = 0;
    for field_no in 0..TInsertSql::get_fields_amount() {
        let update_value = model.get_field_value(field_no);

        if update_value.ignore_if_none && update_value.value.is_none() {
            continue;
        }

        if upsert_columns_to_fill.is_active() {
            let column_name = TInsertSql::get_column_name(field_no);
            upsert_columns_to_fill.push(column_name);
        }

        if field_no_rendered > 0 {
            sql.push(',');
        }

        field_no_rendered += 1;

        update_value.write_value(sql, params);
    }
    sql.push(')');
}
