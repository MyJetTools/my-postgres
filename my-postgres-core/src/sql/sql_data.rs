use rust_extensions::StrOrString;

use super::{sql_string::SqlString, SqlValues};

pub struct SqlData {
    pub sql: String,
    pub values: SqlValues,
}

impl<'s> SqlData {
    pub fn new(sql: impl Into<StrOrString<'static>>, values: SqlValues) -> Self {
        let sql: StrOrString<'static> = sql.into();
        Self {
            sql: sql.to_string(),
            values,
        }
    }

    pub fn builder(sql: impl Into<StrOrString<'static>>) -> Self {
        let sql: StrOrString<'static> = sql.into();
        Self {
            sql: sql.to_string(),
            values: SqlValues::Empty,
        }
    }

    pub fn add_string_value(&mut self, value: impl Into<StrOrString<'static>>) {
        if self.values.is_empty() {
            self.values = SqlValues::Values(Vec::new());
        }
        let value: StrOrString<'static> = value.into();
        self.values.push(SqlString::AsString(value.to_string()));
    }
}

impl<'s> Into<SqlData> for String {
    fn into(self) -> SqlData {
        SqlData {
            sql: self,
            values: SqlValues::Empty,
        }
    }
}
