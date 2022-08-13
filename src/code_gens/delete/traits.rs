use crate::code_gens::SqlValue;

pub trait DeleteCodeGen<'s> {
    fn add_where_field(&mut self, field_name: &str, sql_value: SqlValue<'s>);
}
