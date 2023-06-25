use crate::sql_insert::SqlInsertModel;

use super::{SqlData, SqlValues};

pub fn build_insert_sql<TInsertSql: SqlInsertModel>(
    model: &TInsertSql,
    table_name: &str,
) -> SqlData {
    let mut sql = String::new();

    let mut values = SqlValues::new();

    sql.push_str("INSERT INTO ");
    sql.push_str(table_name);
    TInsertSql::generate_insert_fields(&mut sql);
    sql.push_str(" VALUES ");
    generate_insert_fields_values(model, &mut sql, &mut values);

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
    TInsertSql::generate_insert_fields(&mut sql);
    sql.push_str(" VALUES ");
    generate_insert_fields_values(&model, &mut sql, &mut values);

    SqlData { sql, values }
}

pub fn generate_insert_fields_values<TInsertSql: SqlInsertModel>(
    model: &TInsertSql,
    sql: &mut String,
    params: &mut SqlValues,
) {
    sql.push('(');
    for field_no in 0..TInsertSql::get_fields_amount() {
        let update_value = model.get_field_value(field_no);

        if field_no > 0 {
            sql.push(',');
        }

        update_value.write_value(sql, params, || TInsertSql::get_column_name(field_no));
    }
    sql.push(')');
}
