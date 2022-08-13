use crate::code_gens::{
    insert::InsertCodeGen, insert_or_update::InsertOrUpdateBuilder, update::UpdateBuilder,
};

pub trait SelectEntity {
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
}

pub trait InsertEntity {
    fn populate<'s>(&'s self, sql_builder: &'s mut dyn InsertCodeGen);
}

pub trait UpdateEntity {
    fn populate<'s>(&'s self, sql_builder: &'s mut UpdateBuilder);
}

pub trait InsertOrUpdateEntity {
    fn populate<'s>(&'s self, sql_builder: &'s mut InsertOrUpdateBuilder);
}

pub trait DeleteEntity {
    fn populate<'s>(&'s self, sql_builder: &'s mut dyn crate::code_gens::delete::DeleteCodeGen);
}
