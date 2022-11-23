use crate::sql_value_writer::SqlValueWriter;

pub enum SqlWhereValue<'s> {
    AsValue {
        name: &'static str,
        op: &'static str,
        value: Option<&'s dyn SqlValueWriter<'s>>,
    },
    AsInOperator {
        name: &'static str,
        values: Option<Vec<&'s dyn SqlValueWriter<'s>>>,
    },
}

impl<'s> SqlWhereValue<'s> {
    pub fn to_in_operator<T: SqlValueWriter<'s>>(
        name: &'static str,
        src: Option<&'s Vec<T>>,
    ) -> Self {
        if src.is_none() {
            return Self::AsInOperator { name, values: None };
        }

        let mut values: Vec<&'s dyn SqlValueWriter<'s>> = Vec::new();

        for itm in src.unwrap() {
            values.push(itm);
        }

        Self::AsInOperator {
            name,
            values: Some(values),
        }
    }
}
