use crate::{InputDataValue, SqlWhereData};

pub fn build_where<'s, TWhereModel: SqlWhereData<'s>>(
    sql: &mut String,
    where_model: &'s TWhereModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
) {
    for i in 0..TWhereModel::get_max_fields_amount() {
        if i > 0 {
            sql.push_str(" AND ");
        }

        match where_model.get_field_value(i) {
            InputDataValue::AsString { name, op, value } => {
                sql.push_str(name);
                sql.push_str(op);
                sql.push_str("'");
                sql.push_str(value.as_str());
                sql.push_str("'");
            }
            InputDataValue::AsNonString { name, op, value } => {
                sql.push_str(name);
                sql.push_str(op);
                sql.push_str(value.as_str());
            }
            InputDataValue::AsSqlValue { name, op, value } => {
                params.push(value);
                sql.push_str(name);
                sql.push_str(op);
                sql.push_str("$");
                sql.push_str(params.len().to_string().as_str());
            }
            InputDataValue::AsInOfString { name, values } => {
                if values.len() == 0 {
                    continue;
                }
                if values.len() == 1 {
                    sql.push_str(name);
                    sql.push_str("=");
                    sql.push_str("'");
                    sql.push_str(values.get(0).unwrap());
                    sql.push_str("'");
                }
                sql.push_str(name);
                sql.push_str(" IN (");
                let mut no = 0;
                for value in &values {
                    if no > 0 {
                        sql.push_str(",");
                    }

                    no += 1;
                    sql.push_str("'");
                    sql.push_str(value);
                    sql.push_str("'");
                }
                sql.push_str(")");
            }
            InputDataValue::AsInOfNonString { name, values } => {
                if values.len() == 0 {
                    continue;
                }
                if values.len() == 1 {
                    sql.push_str(name);
                    sql.push_str("=");
                    sql.push_str(values.get(0).unwrap());
                }

                sql.push_str(name);
                sql.push_str(" IN (");
                let mut no = 0;
                for value in &values {
                    if no > 0 {
                        sql.push_str(",");
                    }

                    no += 1;
                    sql.push_str(value);
                }
                sql.push_str(")");
            }
            InputDataValue::AsInOfSqlValue { name, values } => {
                if values.len() == 0 {
                    continue;
                }
                if values.len() == 1 {
                    sql.push_str(name);
                    sql.push_str("=");
                    sql.push('$');
                    params.push(*values.get(0).unwrap());
                    sql.push_str(params.len().to_string().as_str());
                }

                sql.push_str(name);
                sql.push_str(" IN (");
                let mut no = 0;
                for value in values {
                    if no > 0 {
                        sql.push_str(",");
                    }

                    no += 1;
                    sql.push('$');
                    params.push(value);
                    sql.push_str(params.len().to_string().as_str());
                }
                sql.push_str(")");
            }
        }
    }
}
