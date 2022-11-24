use super::{SqlWhereModel, SqlWhereValue};

pub fn build<'s, TSqlWhereModel: SqlWhereModel<'s>>(
    sql: &mut String,
    sql_where_model: &'s TSqlWhereModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
) {
    let mut i = 0;
    for field_no in 0..TSqlWhereModel::get_fields_amount() {
        let value = sql_where_model.get_field_value(field_no);

        if i > 0 {
            sql.push_str(" AND ");
        }

        match value {
            SqlWhereValue::AsValue { name, op, value } => match value {
                crate::SqlValue::Ignore => {}
                crate::SqlValue::Null => {
                    sql.push_str(name);
                    sql.push_str(" IS NULL");
                    i += 1;
                }
                crate::SqlValue::Value { options, value } => {
                    sql.push_str(name);
                    sql.push_str(op);
                    value.write(sql, params, options.as_ref());
                    i += 1;
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
                        crate::SqlValue::Value { options, value } => {
                            sql.push_str(name);
                            sql.push_str(" = ");
                            value.write(sql, params, options.as_ref());
                            i += 1;
                        }
                    }

                    continue;
                }

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
                        crate::SqlValue::Value { options, value } => {
                            value.write(sql, params, options.as_ref());
                            no += 1;
                        }
                    }
                }
            }
        }
    }
}
