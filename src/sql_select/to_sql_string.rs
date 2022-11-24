use rust_extensions::StrOrString;

pub trait ToSqlString {
    fn as_sql(
        &self,
    ) -> (
        StrOrString,
        Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]>,
    );
}

impl ToSqlString for String {
    fn as_sql(
        &self,
    ) -> (
        StrOrString,
        Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]>,
    ) {
        (StrOrString::crate_as_str(self), None)
    }
}

impl<'s> ToSqlString for &'s str {
    fn as_sql(
        &self,
    ) -> (
        StrOrString,
        Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]>,
    ) {
        (StrOrString::crate_as_str(self), None)
    }
}
