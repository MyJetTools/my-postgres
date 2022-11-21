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
}
