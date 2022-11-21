use crate::code_gens::{
    insert::InsertCodeGen, insert_or_update::InsertOrUpdateBuilder, update::UpdateBuilder,
};

pub trait SqlWhereData {
    fn where_line() -> &'static str;
    fn get_param_value(&self, no: usize) -> &(dyn tokio_postgres::types::ToSql + Sync);
}

pub trait BulkSelectEntity {
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
    fn get_select_fields() -> &'static str;
    fn get_line_no(&self) -> i32;
}

pub trait SelectEntity {
    fn from_db_row(row: &tokio_postgres::Row) -> Self;
    fn get_select_fields() -> &'static str;
}

pub trait InsertEntity {
    fn populate<'s>(&'s self, sql_builder: &mut dyn InsertCodeGen<'s>);
}

pub trait UpdateEntity {
    fn populate<'s>(&'s self, sql_builder: &mut UpdateBuilder<'s>);
}

pub trait InsertOrUpdateEntity {
    fn populate<'s>(&'s self, sql_builder: &mut InsertOrUpdateBuilder<'s>);
}

pub trait DeleteEntity {
    fn populate<'s>(&'s self, sql_builder: &mut dyn crate::code_gens::delete::DeleteCodeGen<'s>);
}
