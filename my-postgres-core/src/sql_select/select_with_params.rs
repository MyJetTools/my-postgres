use rust_extensions::StrOrString;

use crate::sql::SqlValues;

use super::ToSqlString;

pub struct SqlWithParams<'s> {
    pub sql: &'s str,
    pub params: SqlValues,
}

pub trait WithSqlParams<'s> {
    fn inject_sql_params_data(&'s self, params: SqlValues) -> SqlWithParams<'s>;
}

impl<'s> WithSqlParams<'s> for String {
    fn inject_sql_params_data(&'s self, params: SqlValues) -> SqlWithParams<'s> {
        SqlWithParams { sql: self, params }
    }
}

impl<'s> WithSqlParams<'s> for &'s str {
    fn inject_sql_params_data(&'s self, params: SqlValues) -> SqlWithParams<'s> {
        SqlWithParams { sql: self, params }
    }
}

impl<'s> ToSqlString<'s> for SqlWithParams<'s> {
    fn as_sql(&'s self) -> (StrOrString<'s>, Option<&'s SqlValues>) {
        (StrOrString::create_as_str(self.sql), Some(&self.params))
    }
}
