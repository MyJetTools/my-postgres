use rust_extensions::StrOrString;

use crate::sql::SqlValues;

use super::ToSqlString;

pub struct SqlWithParams<'s> {
    pub sql: &'s str,
    pub params: SqlValues<'s>,
}

pub trait WithSqlParams<'s> {
    fn inject_sql_params_data(&'s self, params: SqlValues<'s>) -> SqlWithParams;
}

impl<'s> WithSqlParams<'s> for String {
    fn inject_sql_params_data(&'s self, params: SqlValues<'s>) -> SqlWithParams {
        SqlWithParams { sql: self, params }
    }
}

impl<'s> WithSqlParams<'s> for &'s str {
    fn inject_sql_params_data(&'s self, params: SqlValues<'s>) -> SqlWithParams {
        SqlWithParams { sql: self, params }
    }
}

impl<'s> ToSqlString for SqlWithParams<'s> {
    fn as_sql(&self) -> (StrOrString, Option<&SqlValues<'s>>) {
        (StrOrString::create_as_str(self.sql), Some(&self.params))
    }
}
