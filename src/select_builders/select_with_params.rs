pub struct SqlWithParams<'s> {
    pub sql: &'s str,
    pub params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
}

pub trait WithSqlParams<'s> {
    fn with_params(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
    ) -> SqlWithParams;
}

impl<'s> WithSqlParams<'s> for String {
    fn with_params(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
    ) -> SqlWithParams {
        SqlWithParams { sql: self, params }
    }
}

impl<'s> WithSqlParams<'s> for &'s str {
    fn with_params(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
    ) -> SqlWithParams {
        SqlWithParams { sql: self, params }
    }
}
