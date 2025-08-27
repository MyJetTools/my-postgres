use my_postgres::{
    macros::*,
    sql::{SelectBuilder, SqlValues},
    sql_where::NoneWhereModel,
};
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(SelectDbEntity, InsertDbEntity, UpdateDbEntity, TableSchema, Clone, Debug)]
pub struct DiscountCodeDto {
    #[primary_key(0)]
    pub code: String,
    pub amount: f64,
    #[sql_type("timestamp")]
    pub date_created: DateTimeAsMicroseconds,
    #[sql_type("timestamp")]
    pub date_updated: DateTimeAsMicroseconds,
    pub discount_type: i32,
    pub description: String,
    #[sql_type("timestamp")]
    pub date_ends: DateTimeAsMicroseconds,
    #[sql_type("timestamp")]
    pub date_starts: DateTimeAsMicroseconds,
    #[sql_type("json")]
    pub trading_package_ids: Vec<String>,
    pub usage_limit: Option<i32>,
    pub usage_count: i32,
    pub is_active: bool,

    pub woo_commerce_id: Option<i32>,
    #[sql_type("timestamp")]
    #[order_by_desc]
    #[db_index(id=1, index_name: "discount_codes_created_at_idx", is_unique: false, order: "DESC")]
    pub created_at: DateTimeAsMicroseconds,

    #[db_column_name("etag")]
    #[e_tag]
    pub e_tag: i64,

    #[default_value("false")]
    #[db_index(id=2, index_name: "discount_codes_code_idx", is_unique: false, order: "DESC")]
    pub is_updated_in_integration: bool,
}

#[test]
fn test() {
    let select_builder = SelectBuilder::from_select_model::<DiscountCodeDto>();

    let mut sql = String::new();
    let mut values = SqlValues::new();
    select_builder.build_select_sql(&mut sql, &mut values, "Test", NoneWhereModel::new());

    println!(
        "sql: {}",
        r#"SELECT code,amount,(extract(EPOCH FROM date_created) * 1000000)::bigint as "date_created",(extract(EPOCH FROM date_updated) * 1000000)::bigint as "date_updated",discount_type,description,(extract(EPOCH FROM date_ends) * 1000000)::bigint as "date_ends",(extract(EPOCH FROM date_starts) * 1000000)::bigint as "date_starts",trading_package_ids #>> '{}' as "trading_package_ids",usage_limit,usage_count,is_active,woo_commerce_id,(extract(EPOCH FROM created_at) * 1000000)::bigint as "created_at",etag,is_updated_in_integration FROM Test ORDER BY created_at DESC"#
    );
}
