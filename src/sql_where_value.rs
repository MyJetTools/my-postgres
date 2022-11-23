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
