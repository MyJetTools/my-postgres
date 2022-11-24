use crate::{sql_value_writer::SqlValueWriter, SqlValue};

pub enum SqlWhereValue<'s> {
    AsValue {
        name: &'static str,
        op: &'static str,
        value: SqlValue<'s>,
    },
    AsInOperator {
        name: &'static str,
        values: Option<Vec<SqlValue<'s>>>,
    },
}

impl<'s> SqlWhereValue<'s> {
    pub fn to_in_operator<T: SqlValueWriter<'s>>(
        name: &'static str,
        src: &'s Option<Vec<T>>,
    ) -> Self {
        match src {
            Some(src) => {
                let mut values: Vec<SqlValue<'s>> = Vec::new();

                for itm in src {
                    values.push(SqlValue::Value {
                        options: None,
                        value: itm,
                    });
                }

                Self::AsInOperator {
                    name,
                    values: Some(values),
                }
            }
            None => Self::AsInOperator { name, values: None },
        }
    }
}
