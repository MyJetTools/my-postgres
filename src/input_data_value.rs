pub enum InputDataValue<'s> {
    AsString {
        name: &'static str,
        value: String,
    },
    AsNonString {
        name: &'static str,
        value: String,
    },
    AsSqlValue {
        name: &'static str,
        value: &'s (dyn tokio_postgres::types::ToSql + Sync),
    },
}
