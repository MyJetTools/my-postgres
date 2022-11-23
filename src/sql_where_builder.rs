use crate::SqlWhereData;

pub fn build<'s, TWhereModel: SqlWhereData<'s>>(
    sql: &mut String,
    where_model: &'s TWhereModel,
    params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
) {
    for i in 0..TWhereModel::get_max_fields_amount() {
        if i > 0 {
            sql.push_str(" AND ");
        }

        match where_model.get_field_value(i) {
            crate::SqlWhereValue::AsValue { name, op, value } => {
                if let Some(value) = value {
                    sql.push_str(name);
                    sql.push_str(op);
                    value.write(sql, params);
                }
            }
            crate::SqlWhereValue::AsInOperator { name, values } => {
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
        };
    }
}
