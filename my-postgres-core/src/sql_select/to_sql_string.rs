use rust_extensions::StrOrString;

use crate::sql::SqlValues;

pub trait ToSqlString {
    fn as_sql(&self) -> (StrOrString, Option<&SqlValues>);
}

impl ToSqlString for String {
    fn as_sql(&self) -> (StrOrString, Option<&SqlValues>) {
        (StrOrString::create_as_str(self), None)
    }
}

impl<'s> ToSqlString for &'s str {
    fn as_sql(&self) -> (StrOrString, Option<&SqlValues>) {
        (StrOrString::create_as_str(self), None)
    }
}
