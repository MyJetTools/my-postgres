use rust_extensions::StrOrString;

use crate::SelectEntity;

pub trait ToSqlString<TEntity: SelectEntity> {
    fn as_sql(
        &self,
    ) -> (
        StrOrString,
        Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]>,
    );
}

impl<TEntity: SelectEntity> ToSqlString<TEntity> for String {
    fn as_sql(
        &self,
    ) -> (
        StrOrString,
        Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]>,
    ) {
        let result = crate::sql_formatter::format_sql(StrOrString::crate_as_str(self), || {
            TEntity::get_select_fields()
        });

        (result, None)
    }
}

impl<'s, TEntity: SelectEntity> ToSqlString<TEntity> for &'s str {
    fn as_sql(
        &self,
    ) -> (
        StrOrString,
        Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]>,
    ) {
        let result = crate::sql_formatter::format_sql(StrOrString::crate_as_str(self), || {
            TEntity::get_select_fields()
        });

        (result, None)
    }
}
