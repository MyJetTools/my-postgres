use rust_extensions::StrOrString;

use crate::sql::SqlValues;

use super::SqlWhereModel;

pub struct StaticLineWhereModel(StrOrString<'static>);

impl StaticLineWhereModel {
    pub fn new(value: impl Into<StrOrString<'static>>) -> Self {
        Self(value.into())
    }
}

impl SqlWhereModel for StaticLineWhereModel {
    fn fill_where_component(&self, sql: &mut String, _params: &mut SqlValues) {
        sql.push_str(self.0.as_str());
    }

    fn get_limit(&self) -> Option<usize> {
        None
    }

    fn get_offset(&self) -> Option<usize> {
        None
    }

    fn has_conditions(&self) -> bool {
        self.0.as_str().len() > 0
    }
}
