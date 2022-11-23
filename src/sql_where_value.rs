use crate::sql_where_writer::SqlWhereValueWriter;

pub enum SqlWhereValue<'s> {
    AsValue {
        name: &'static str,
        op: &'static str,
        value: &'s dyn SqlWhereValueWriter<'s>,
    },
    AsInOperator {
        name: &'static str,
        values: Vec<&'s dyn SqlWhereValueWriter<'s>>,
    },
}

impl<'s> SqlWhereValue<'s> {
    pub fn to_in_operator<T: SqlWhereValueWriter<'s>>(name: &'static str, src: &'s Vec<T>) -> Self {
        let mut values: Vec<&'s dyn SqlWhereValueWriter<'s>> = Vec::new();

        for itm in src {
            values.push(itm);
        }

        Self::AsInOperator { name, values }
    }
}
