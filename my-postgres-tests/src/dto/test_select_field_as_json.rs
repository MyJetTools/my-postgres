use my_postgres::macros::*;
use serde::*;

#[derive(Serialize, Deserialize, MyPostgresJsonModel, Clone, Debug)]
pub struct LifetimeMetaData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifetime_discount_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_group_live: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_group_demo: Option<String>,
}

#[derive(SelectDbEntity, InsertDbEntity, UpdateDbEntity, TableSchema, Clone, Debug)]
pub struct OptionalJson {
    pub lifetime_metadata: Option<LifetimeMetaData>,
}
