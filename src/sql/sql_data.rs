use super::SqlValues;

pub struct SqlData {
    pub sql: String,
    pub values: SqlValues,
}

impl<'s> SqlData {
    pub fn new(sql: String, values: SqlValues) -> Self {
        Self { sql, values }
    }

    pub fn get_sql_as_process_name(&self) -> &str {
        &self.sql
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
