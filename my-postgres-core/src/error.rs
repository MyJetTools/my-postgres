use std::time::Duration;

#[derive(Debug)]
pub enum MyPostgresError {
    NoConnection,
    SingleRowRequestReturnedMultipleRows(usize),
    PostgresError(tokio_postgres::Error),
    Other(String),
    TimeOut(Duration),
    ConnectionNotStartedYet,
}

impl MyPostgresError {
    pub fn is_connection_not_started_yet(&self) -> bool {
        match self {
            Self::ConnectionNotStartedYet => true,
            _ => false,
        }
    }
}

impl From<tokio_postgres::Error> for MyPostgresError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::PostgresError(error)
    }
}
