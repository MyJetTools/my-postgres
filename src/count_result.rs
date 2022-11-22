pub trait CountResult {
    fn get_postgres_type() -> &'static str;
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
}

impl CountResult for u64 {
    fn get_postgres_type() -> &'static str {
        "bigint"
    }

    fn from_db_row(row: &tokio_postgres::Row) -> Self {
        let result: i64 = row.get(0);
        result as u64
    }
}

impl CountResult for i32 {
    fn get_postgres_type() -> &'static str {
        "int"
    }

    fn from_db_row(row: &tokio_postgres::Row) -> Self {
        row.get(0)
    }
}

impl CountResult for usize {
    fn get_postgres_type() -> &'static str {
        "bigint"
    }

    fn from_db_row(row: &tokio_postgres::Row) -> Self {
        let result: i64 = row.get(0);
        result as usize
    }
}

impl CountResult for i16 {
    fn get_postgres_type() -> &'static str {
        "smallint"
    }

    fn from_db_row(row: &tokio_postgres::Row) -> Self {
        row.get(0)
    }
}
