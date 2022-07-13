use crate::code_gens::SqlValue;

pub trait InsertCodeGen {
    fn append_field(&mut self, field_name: &str, value: SqlValue);
}
