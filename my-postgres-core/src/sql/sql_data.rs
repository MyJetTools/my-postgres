use super::SqlValues;

pub struct SqlData {
    pub sql: String,
    pub values: SqlValues,
}

impl<'s> SqlData {
    pub fn new(sql: String, values: SqlValues) -> Self {
        Self { sql, values }
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
