use crate::sql_where::SqlWhereModel;

impl<'s> SqlWhereModel for &'s str {
    fn fill_where_component(&self, sql: &mut String, _params: &mut crate::sql::SqlValues) {
        sql.push_str(self);
    }

    fn get_limit(&self) -> Option<usize> {
        None
    }

    fn get_offset(&self) -> Option<usize> {
        None
    }

    fn has_conditions(&self) -> bool {
        self.len() > 0
    }
}
