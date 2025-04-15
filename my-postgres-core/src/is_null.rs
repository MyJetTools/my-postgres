#[derive(Debug, Clone, Copy)]
pub enum IsNull {
    Yes,
    No,
}

impl Into<tokio_postgres::types::IsNull> for IsNull {
    fn into(self) -> tokio_postgres::types::IsNull {
        match self {
            IsNull::Yes => tokio_postgres::types::IsNull::Yes,
            IsNull::No => tokio_postgres::types::IsNull::No,
        }
    }
}

impl Into<IsNull> for tokio_postgres::types::IsNull {
    fn into(self) -> IsNull {
        match self {
            tokio_postgres::types::IsNull::Yes => IsNull::Yes,
            tokio_postgres::types::IsNull::No => IsNull::No,
        }
    }
}
