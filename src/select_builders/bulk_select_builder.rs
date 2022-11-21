use crate::{InputDataValue, SqlWhereData};

pub struct BulkSelectBuilder<'s, TIn: SqlWhereData<'s>> {
    pub input_params: Vec<TIn>,
    pub table_name: &'s str,
}

impl<'s, TIn: SqlWhereData<'s>> BulkSelectBuilder<'s, TIn> {
    pub fn new(table_name: &'s str, input_params: Vec<TIn>) -> Self {
        Self {
            table_name,
            input_params,
        }
    }

    pub fn build_sql(
        &'s self,
        select_part: &str,
    ) -> (String, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        let mut line_no = 0;

        for input_param in &self.input_params {
            if line_no > 0 {
                sql.push_str("UNION ALL\n");
            }
            line_no += 1;

            sql.push_str("SELECT ");
            sql.push_str(line_no.to_string().as_str());
            sql.push_str("::int as line_no, ");
            sql.push_str(select_part);
            sql.push_str(" FROM ");
            sql.push_str(self.table_name);
            sql.push_str(" WHERE ");

            for i in 0..TIn::get_max_fields_amount() {
                if i > 0 {
                    sql.push_str(" AND ");
                }

                match input_param.get_field_value(i) {
                    InputDataValue::AsString { name, value } => {
                        sql.push_str(name);
                        sql.push_str(" = '");
                        sql.push_str(value.as_str());
                        sql.push_str("'");
                    }
                    InputDataValue::AsNonString { name, value } => {
                        sql.push_str(name);
                        sql.push_str(" = ");
                        sql.push_str(value.as_str());
                    }
                    InputDataValue::AsSqlValue { name, value } => {
                        params.push(value);
                        sql.push_str(name);
                        sql.push_str(" = $");
                        sql.push_str(params.len().to_string().as_str());
                    }
                }
            }

            sql.push('\n');
            line_no += 1;
        }

        println!("sql: {}", sql);
        println!("params: {:?}", params);

        (sql, params)
    }
}

#[cfg(test)]
#[cfg(not(feature = "with-logs-and-telemetry"))]
mod tests {

    use crate::{BulkSelectBuilder, InputDataValue, SqlWhereData};

    #[test]
    fn test_build_sql() {
        struct Param {
            q1: String,
            q2: String,
            q3: i64,
        }

        impl<'s> SqlWhereData<'s> for Param {
            fn get_field_value(&self, no: usize) -> InputDataValue {
                match no {
                    0 => InputDataValue::AsSqlValue {
                        name: "q1",
                        value: &self.q1,
                    },

                    1 => InputDataValue::AsSqlValue {
                        name: "q2",
                        value: &self.q2,
                    },
                    2 => InputDataValue::AsSqlValue {
                        name: "q3",
                        value: &self.q3,
                    },
                    _ => panic!("Unexpected param no: {}", no),
                }
            }

            fn get_max_fields_amount() -> usize {
                3
            }
        }

        let params = vec![
            Param {
                q1: "1".to_string(),
                q2: "2".to_string(),
                q3: 30,
            },
            Param {
                q1: "3".to_string(),
                q2: "4".to_string(),
                q3: 40,
            },
        ];

        let bulk_select = BulkSelectBuilder::new("test", params);

        let (result, values) = bulk_select.build_sql("*");
        println!("{}", result);
        println!("{:?}", values);
    }
}
