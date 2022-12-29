use crate::SqlValue;

pub trait SqlWhereModel<'s> {
    fn fill_where(&'s self, sql: &mut String, params: &mut Vec<SqlValue<'s>>);

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;
}
