use my_postgres::macros::*;

#[derive(TableSchema)]
pub struct EntityWithJsonBArray {
    pub id: i32,
    #[sql_type("jsonb")]
    #[db_index(id: 0, index_name: "jsonb_array_index", is_unique: false, order: "ASC")]
    pub jsonb_array: Vec<String>,
}
