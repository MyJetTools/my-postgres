use my_postgres::macros::*;
use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::*;

#[derive(SelectDbEntity, InsertDbEntity, UpdateDbEntity, TableSchema, Clone, Debug)]
pub struct ContractDto {
    #[primary_key(0)]
    pub id: String,
    pub trading_package_id: String,

    #[db_index(id=1, index_name: "contract_id_idx", is_unique: true, order: "DESC")]
    #[order_by_desc]
    pub contract_id: i64,

    #[db_index(id=2, index_name: "contract_client_id_idx", is_unique: false, order: "ASC")]
    pub client_id: String,

    #[db_index(id=3, index_name: "contract_aggregated_id_idx", is_unique: true, order: "ASC")]
    pub trader_account_aggregated_id: String,

    pub sign_status: Option<ContractSignStatusDto>,

    pub live_account_status: ContractLiveAccountStatusDto,

    #[default_value("false")]
    #[db_index(id=5, index_name: "contract_is_signed_idx", is_unique: false, order: "ASC")]
    pub is_signed_contract_received: bool,

    #[sql_type("timestamp")]
    #[db_index(id=6, index_name: "contract_send_date_idx", is_unique: false, order: "ASC")]
    pub contract_send_date: Option<DateTimeAsMicroseconds>,

    #[sql_type("timestamp")]
    #[db_index(id=7, index_name: "contract_send_date_idx", is_unique: false, order: "ASC")]
    pub contract_sign_finish_date: Option<DateTimeAsMicroseconds>,

    #[sql_type("timestamp")]
    pub created_at: DateTimeAsMicroseconds,

    #[sql_type("timestamp")]
    pub updated_at: DateTimeAsMicroseconds,

    #[sql_type("json")]
    pub comments: Option<Vec<ContractCommentDto>>,

    pub brand: String,

    #[sql_type("json")]
    pub logs: Option<Vec<ContractLogsDto>>,
}

#[derive(Serialize, Deserialize, MyPostgresJsonModel, Clone, Debug)]
pub struct ContractCommentDto {
    pub created_at: i64,
    pub comment: String,
    pub officer_id: String,
}

#[derive(Serialize, Deserialize, MyPostgresJsonModel, Clone, Debug)]
pub struct ContractLogsDto {
    pub created_at: i64,
    pub comment: String,
    pub officer_id: String,
}

#[derive(
    DbEnumAsI32,
    Copy,
    Clone,
    Default,
    PartialEq,
    PartialOrd,
    Ord,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Debug,
)]
pub enum ContractSignStatusDto {
    #[default]
    #[enum_case(0)]
    NotInit,
    #[enum_case(1)]
    Pending,
    #[enum_case(2)]
    Success,
    #[enum_case(3)]
    Failed,
    #[enum_case(4)]
    FailedAndBlock,
}

#[derive(
    DbEnumAsI32,
    Copy,
    Clone,
    Default,
    PartialEq,
    PartialOrd,
    Ord,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Debug,
)]
pub enum ContractLiveAccountStatusDto {
    #[default]
    #[enum_case(0)]
    NotInit,
    #[enum_case(1)]
    GrantedLiveAccount,
    #[enum_case(2)]
    Rejected,
    #[enum_case(3)]
    RejectedAndBlocked,
}
