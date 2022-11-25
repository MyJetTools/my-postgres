use crate::{sql_value_writer::SqlValueWriter, SqlValue};

pub enum SqlWhereValue<'s> {
    AsValue {
        name: &'static str,
        op: &'static str,
        value: SqlValue<'s>,
    },
    AsInOperator {
        name: &'static str,
        values: Vec<SqlValue<'s>>,
    },
}

impl<'s> SqlWhereValue<'s> {
    pub fn to_in_operator<T: SqlValueWriter<'s>>(name: &'static str, src: &'s Vec<T>) -> Self {
        let mut values: Vec<SqlValue<'s>> = Vec::new();

        for itm in src {
            values.push(SqlValue::Value {
                options: None,
                value: itm,
            });
        }

        Self::AsInOperator { name, values }
    }
}
