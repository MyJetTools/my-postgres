use rust_extensions::StrOrString;

use crate::{SelectEntity, ToSqlString};

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

impl<'s, TEntity: SelectEntity> ToSqlString<TEntity> for SqlWithParams<'s> {
    fn as_sql(&self) -> StrOrString {
        crate::sql_formatter::format_sql(StrOrString::crate_as_str(self.sql), || {
            TEntity::get_select_fields()
        })
    }

    fn get_params_data(&self) -> Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]> {
        Some(self.params)
    }
}
