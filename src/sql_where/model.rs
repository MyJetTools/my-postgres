use crate::SqlValueToWrite;

pub trait SqlWhereModel<'s> {
    fn fill_where(&'s self, sql: &mut String, params: &mut Vec<SqlValueToWrite<'s>>);

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;
}
