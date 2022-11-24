use super::{SqlWhereModel, SqlWhereValue};

pub fn build<'s, TSqlWhereModel: SqlWhereModel<'s>>(
    sql: &mut String,
    sql_where_model: &'s TSqlWhereModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
) {
    let mut i = 0;
    for field_no in 0..TSqlWhereModel::get_fields_amount() {
        let value = sql_where_model.get_field_value(field_no);

        if value.is_ignore() {
            continue;
        }

        if i > 0 {
            sql.push_str(" AND ");
        }
        i += 1;

        match value {
            SqlWhereValue::Ignore => {}
            SqlWhereValue::Null(name) => {
                sql.push_str(name);
                sql.push_str(" IS NULL");
            }
            SqlWhereValue::AsValue { name, op, value } => {
                if let Some(value) = value {
                    sql.push_str(name);
                    sql.push_str(op);
                    value.write(sql, params);
                }
            }
            SqlWhereValue::AsInOperator { name, values } => {
                if values.is_none() {
                    continue;
                }

                let values = values.unwrap();
                if values.len() == 0 {
                    continue;
                }

                if values.len() == 1 {
                    sql.push_str(name);
                    sql.push_str(" = ");
                    values.get(0).unwrap().write(sql, params);
                    continue;
                }

                sql.push_str(name);
                sql.push_str(" IN (");
                let mut no = 0;
                for value in values {
                    if no > 0 {
                        sql.push(',');
                    }
                    no += 1;
                    value.write(sql, params);
                }
            }
        }
    }
}
