use rust_extensions::StrOrString;

use crate::sql::SqlValues;

pub trait ToSqlString<'s> {
    fn as_sql(&'s self) -> (StrOrString<'s>, Option<&'s SqlValues>);
}

impl<'s> ToSqlString<'s> for String {
    fn as_sql(&'s self) -> (StrOrString<'s>, Option<&'s SqlValues>) {
        (StrOrString::create_as_str(self), None)
    }
}

impl<'s> ToSqlString<'s> for &'s str {
    fn as_sql(&'s self) -> (StrOrString<'s>, Option<&'s SqlValues>) {
        (StrOrString::create_as_str(self), None)
    }
}
