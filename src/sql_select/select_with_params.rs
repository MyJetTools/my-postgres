use rust_extensions::StrOrString;

use super::ToSqlString;

pub struct SqlWithParams<'s> {
    pub sql: &'s str,
    pub params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
}

pub trait WithSqlParams<'s> {
    fn inject_sql_params_data(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
    ) -> SqlWithParams;
}

impl<'s> WithSqlParams<'s> for String {
    fn inject_sql_params_data(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
    ) -> SqlWithParams {
        SqlWithParams { sql: self, params }
    }
}

impl<'s> WithSqlParams<'s> for &'s str {
    fn inject_sql_params_data(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
    ) -> SqlWithParams {
        SqlWithParams { sql: self, params }
    }
}

impl<'s> ToSqlString for SqlWithParams<'s> {
    fn as_sql(
        &self,
    ) -> (
        StrOrString,
        Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]>,
    ) {
        (StrOrString::create_as_str(self.sql), Some(self.params))
    }
}
