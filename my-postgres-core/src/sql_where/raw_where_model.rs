use crate::sql::SqlValues;

use super::SqlWhereModel;

#[derive(Debug)]
pub struct StaticLineWhereModel<'s> {
    data: &'s str,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'s> StaticLineWhereModel<'s> {
    pub fn new(value: &'s str) -> Self {
        Self {
            data: value,
            limit: None,
            offset: None,
        }
    }

    pub fn set_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn set_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}

impl<'s> SqlWhereModel for StaticLineWhereModel<'s> {
    fn fill_where_component(&self, sql: &mut String, _params: &mut SqlValues) {
        sql.push_str(self.data);
    }

    fn get_limit(&self) -> Option<usize> {
        self.limit
    }

    fn get_offset(&self) -> Option<usize> {
        self.offset
    }

    fn has_conditions(&self) -> bool {
        self.data.len() > 0
    }
}
