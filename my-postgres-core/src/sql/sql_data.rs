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

    pub fn add_string_value(mut self, value: impl Into<StrOrString<'static>>) -> Self {
        if self.values.is_empty() {
            self.values = SqlValues::Values(Vec::new());
        }
        let value: StrOrString<'static> = value.into();
        self.values.push(SqlString::from_str(value.as_str()));

        self
    }

    pub fn add_small_int_value(mut self, value: i16) -> Self {
        if self.values.is_empty() {
            self.values = SqlValues::Values(Vec::new());
        }

        self.values.push(SqlString::NonStrValue(
            super::sql_string::NonStringValue::SmallInt(value),
        ));

        self
    }

    pub fn add_int_value(mut self, value: i32) -> Self {
        if self.values.is_empty() {
            self.values = SqlValues::Values(Vec::new());
        }

        self.values.push(SqlString::NonStrValue(
            super::sql_string::NonStringValue::Integer(value),
        ));

        self
    }

    pub fn add_big_int_value(mut self, value: i64) -> Self {
        if self.values.is_empty() {
            self.values = SqlValues::Values(Vec::new());
        }

        self.values.push(SqlString::NonStrValue(
            super::sql_string::NonStringValue::BigInt(value),
        ));

        self
    }

    pub fn add_float_value(mut self, value: f32) -> Self {
        if self.values.is_empty() {
            self.values = SqlValues::Values(Vec::new());
        }

        self.values.push(SqlString::NonStrValue(
            super::sql_string::NonStringValue::Float(value),
        ));

        self
    }

    pub fn add_double_value(mut self, value: f64) -> Self {
        if self.values.is_empty() {
            self.values = SqlValues::Values(Vec::new());
        }

        self.values.push(SqlString::NonStrValue(
            super::sql_string::NonStringValue::Double(value),
        ));

        self
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

#[cfg(test)]
mod tests {
    use super::SqlData;

    #[test]
    fn example() {
        let _sql = SqlData::builder("SELECT * FROM TABLE WHERE a=$1 AND b=$2")
            .add_string_value("26")
            .add_string_value("66");
    }
}
