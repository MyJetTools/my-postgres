use crate::code_gens::{
    insert::InsertBuilder, insert_or_update::InsertOrUpdateBuilder, update::UpdateBuilder,
};

pub trait SelectEntity {
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
}

pub trait InsertEntity {
    fn populate(self, sql_builder: &mut InsertBuilder);
}

pub trait UpdateEntity {
    fn populate(self, sql_builder: &mut UpdateBuilder);
}

pub trait InsertOrUpdateEntity {
    fn populate(self, sql_builder: &mut InsertOrUpdateBuilder);
}

pub trait DeleteEntity {
    fn populate(self, sql_builder: &mut dyn crate::code_gens::delete::DeleteCodeGen);
}
