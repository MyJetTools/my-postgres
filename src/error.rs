#[derive(Debug)]
pub enum MyPostgressError {
    NoConnection,
    PostgresError(tokio_postgres::Error),
}

impl From<tokio_postgres::Error> for MyPostgressError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::PostgresError(error)
    }
}
