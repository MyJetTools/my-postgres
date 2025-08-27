use my_postgres::macros::*;
use serde::{Deserialize, Serialize};

#[derive(TableSchema)]
pub struct EntityWithJsonBArray {
    pub id: i32,
    #[sql_type("jsonb")]
    #[db_index(id: 0, index_name: "jsonb_array_index", is_unique: false, order: "ASC")]
    pub jsonb_array: Vec<String>,

    #[sql_type("json")]
    pub jsonb_array_opt: Option<Vec<MyType>>,
}

#[derive(Serialize, Deserialize, MyPostgresJsonModel, Clone, Debug)]
pub struct MyType {
    pub created_at: i64,
    pub comment: String,
    pub officer_id: String,
}
