use crate::sql_where::SqlWhereModel;

pub struct BulkSelectBuilder<'s, TWhereModel: SqlWhereModel<'s>> {
    pub where_models: Vec<TWhereModel>,
    pub table_name: &'s str,
}

impl<'s, TWhereModel: SqlWhereModel<'s>> BulkSelectBuilder<'s, TWhereModel> {
    pub fn new(table_name: &'s str, where_models: Vec<TWhereModel>) -> Self {
        Self {
            table_name,
            where_models,
        }
    }

    pub fn build_sql<TBuildSelect: Fn(&mut String)>(
        &'s self,
        build_select_part: TBuildSelect,
    ) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        let mut line_no = 0;

        for where_model in &self.where_models {
            if line_no > 0 {
                sql.push_str("UNION ALL\n");
            }

            sql.push_str("SELECT ");
            sql.push_str(line_no.to_string().as_str());
            sql.push_str("::int as line_no, ");
            build_select_part(&mut sql);
            sql.push_str(" FROM ");
            sql.push_str(self.table_name);
            sql.push_str(" WHERE ");

            where_model.fill_where(&mut sql, &mut params);

            sql.push('\n');
            line_no += 1;
        }

        (sql, params)
    }
}

/*
#[cfg(test)]
#[cfg(not(feature = "with-logs-and-telemetry"))]
mod tests {

    use crate::{sql_select::BulkSelectBuilder, sql_where::SqlWhereModel};

    #[test]
    fn test_build_sql() {
        struct Param {
            q1: String,
            q2: String,
            q3: i64,
        }

        impl<'s> SqlWhereModel<'s> for Param {
            fn get_field_value(&'s self, no: usize) -> SqlWhereValue<'s> {
                match no {
                    0 => SqlWhereValue::AsValue {
                        name: "q1",
                        op: " = ",
                        value: crate::SqlValue::Value {
                            sql_type: None,
                            value: &self.q1,
                        },
                    },

                    1 => SqlWhereValue::AsValue {
                        name: "q2",
                        op: " = ",
                        value: crate::SqlValue::Value {
                            sql_type: None,
                            value: &self.q2,
                        },
                    },
                    2 => SqlWhereValue::AsValue {
                        name: "q3",
                        op: " = ",
                        value: crate::SqlValue::Value {
                            sql_type: None,
                            value: &self.q3,
                        },
                    },
                    _ => panic!("Unexpected param no: {}", no),
                }
            }

            fn get_fields_amount() -> usize {
                3
            }

            fn get_limit(&self) -> Option<usize> {
                None
            }

            fn get_offset(&self) -> Option<usize> {
                None
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

        let (result, values) = bulk_select.build_sql(|sql| sql.push('*'));
        println!("{}", result);
        println!("{:?}", values);
    }
}
 */
