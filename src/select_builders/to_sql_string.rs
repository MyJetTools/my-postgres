use rust_extensions::StrOrString;

use crate::SelectEntity;

pub trait ToSqlString<TEntity: SelectEntity> {
    fn as_sql(&self) -> StrOrString;
    fn get_params_data(&self) -> Option<&Vec<&(dyn tokio_postgres::types::ToSql + Sync)>>;
}

impl<TEntity: SelectEntity> ToSqlString<TEntity> for String {
    fn as_sql(&self) -> StrOrString {
        StrOrString::crate_as_str(self)
    }

    fn get_params_data(&self) -> Option<&Vec<&(dyn tokio_postgres::types::ToSql + Sync)>> {
        None
    }
}

impl<'s, TEntity: SelectEntity> ToSqlString<TEntity> for &'s str {
    fn as_sql(&self) -> StrOrString {
        StrOrString::crate_as_str(self)
    }

    fn get_params_data(&self) -> Option<&Vec<&(dyn tokio_postgres::types::ToSql + Sync)>> {
        None
    }
}
