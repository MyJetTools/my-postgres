use crate::sql::SqlValues;

use super::SqlWhereModel;

pub struct NoneWhereModel;

impl NoneWhereModel {
    pub fn new() -> Option<&'static Self> {
        None
    }
}

impl SqlWhereModel for NoneWhereModel {
    fn fill_where_component(&self, _sql: &mut String, _params: &mut SqlValues) {}

    fn get_limit(&self) -> Option<usize> {
        None
    }

    fn get_offset(&self) -> Option<usize> {
        None
    }

    fn has_conditions(&self) -> bool {
        false
    }
}
