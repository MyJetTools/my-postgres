use super::{SqlWhereModel, SqlWhereValue};

pub fn build<'s, TSqlWhereModel: SqlWhereModel<'s>>(
    sql: &mut String,
    sql_where_model: &'s TSqlWhereModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
) {
    let mut i = 0;
    for field_no in 0..TSqlWhereModel::get_fields_amount() {
        let value = sql_where_model.get_field_value(field_no);

        match value {
            SqlWhereValue::AsValue { name, op, value } => match value {
                crate::SqlValue::Ignore => {}
                crate::SqlValue::Null => {
                    if i > 0 {
                        sql.push_str(" AND ");
                    }
                    i += 1;
                    sql.push_str(name);
                    sql.push_str(" IS NULL");
                }
                crate::SqlValue::Value { sql_type, value } => {
                    if i > 0 {
                        sql.push_str(" AND ");
                    }
                    i += 1;
                    sql.push_str(name);
                    sql.push_str(op);
                    value.write(sql, params, sql_type);
                }
            },
            SqlWhereValue::AsInOperator { name, values } => {
                if values.is_none() {
                    continue;
                }

                let values = values.unwrap();

                if values.len() == 0 {
                    continue;
                }

                if values.len() == 1 {
                    match values.get(0).unwrap() {
                        crate::SqlValue::Ignore => {}
                        crate::SqlValue::Null => {}
                        crate::SqlValue::Value { sql_type, value } => {
                            if i > 0 {
                                sql.push_str(" AND ");
                            }
                            i += 1;
                            sql.push_str(name);
                            sql.push_str(" = ");
                            value.write(sql, params, *sql_type);
                            i += 1;
                        }
                    }

                    continue;
                }
                if i > 0 {
                    sql.push_str(" AND ");
                }
                i += 1;

                sql.push_str(name);
                sql.push_str(" IN (");
                let mut no = 0;
                for value in values {
                    if no > 0 {
                        sql.push(',');
                    }

                    match value {
                        crate::SqlValue::Ignore => {}
                        crate::SqlValue::Null => {}
                        crate::SqlValue::Value { sql_type, value } => {
                            value.write(sql, params, sql_type);
                            no += 1;
                        }
                    }
                }

                sql.push(')');
            }
        }
    }
}
