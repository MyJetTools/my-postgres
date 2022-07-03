use crate::code_gens::insert_or_update::InsertOrUpdateBuilder;

pub trait SelectEntity {
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
}

pub trait InsertOrUpdateEntity {
    fn populate(&self, sql_builder: &mut InsertOrUpdateBuilder);
}

pub trait DeleteEntity {
    fn populate(self, sql_builder: &mut dyn crate::code_gens::delete::DeleteCodeGen);
}
