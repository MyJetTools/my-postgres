pub type DbRow = tokio_postgres::Row;

/*
todo!("Delete")
pub struct DbRow<'s> {
    row: &'s tokio_postgres::Row,
}

impl<'s> DbRow<'s> {
    pub fn new(row: &'s tokio_postgres::Row) -> Self {
        Self { row }
    }

    pub fn get<I, T>(&'s self, idx: I) -> T
    where
        I: RowIndex + std::fmt::Display,
        T: FromSql<'s>,
    {
        self.row.get(idx)
    }
}
 */
