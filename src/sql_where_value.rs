use crate::sql_value_writer::SqlValueWriter;

pub enum SqlWhereValue<'s> {
    AsValue {
        name: &'static str,
        op: &'static str,
        value: &'s dyn SqlValueWriter<'s>,
    },
    AsInOperator {
        name: &'static str,
        values: Vec<&'s dyn SqlValueWriter<'s>>,
    },
}

impl<'s> SqlWhereValue<'s> {
    pub fn to_in_operator<T: SqlValueWriter<'s>>(name: &'static str, src: &'s Vec<T>) -> Self {
        let mut values: Vec<&'s dyn SqlValueWriter<'s>> = Vec::new();

        for itm in src {
            values.push(itm);
        }

        Self::AsInOperator { name, values }
    }
}
