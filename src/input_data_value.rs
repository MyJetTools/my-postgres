pub enum InputDataValue<'s> {
    AsString {
        name: &'static str,
        op: &'static str,
        value: String,
    },
    AsNonString {
        name: &'static str,
        op: &'static str,
        value: String,
    },
    AsSqlValue {
        name: &'static str,
        op: &'static str,
        value: &'s (dyn tokio_postgres::types::ToSql + Sync),
    },
    AsInOfString {
        name: &'static str,
        values: Vec<String>,
    },
    AsInOfNonString {
        name: &'static str,
        values: Vec<String>,
    },

    AsInOfSqlValue {
        name: &'static str,
        values: Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
    },
}
