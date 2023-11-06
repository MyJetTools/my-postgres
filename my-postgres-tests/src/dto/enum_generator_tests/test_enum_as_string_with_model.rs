use my_postgres::macros::*;
use serde::{Deserialize, Serialize};

#[derive(SelectDbEntity, TableSchema)]
pub struct TestDbEntity {
    pub id: i32,
    pub value: PaymentDetailsDto,
}

#[derive(DbEnumAsStringWithModel, Clone, Serialize, Deserialize)]
pub enum PaymentDetailsDto {
    #[enum_case("NotSelected")]
    NotSelected(NotSelectedPaymentDetailsDto),
    #[enum_case("BankTransfer")]
    BankTransfer(BankTransferDetailsDto),
    #[enum_case("CryptoWithdrawal")]
    CryptoWithdrawal(CryptoWithdrawalDetailsDto),
}

#[derive(Serialize, Deserialize, MyPostgresJsonModel, Clone)]
pub struct NotSelectedPaymentDetailsDto {}

#[derive(Serialize, Deserialize, MyPostgresJsonModel, Clone)]
pub struct BankTransferDetailsDto {
    pub bank_name: String,
}

#[derive(Serialize, Deserialize, MyPostgresJsonModel, Clone)]
pub struct CryptoWithdrawalDetailsDto {
    pub crypto_wallet_address: String,
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SelectBuilder, sql_where::NoneWhereModel};

    use crate::dto::enum_generator_tests::test_enum_as_string_with_model::TestDbEntity;

    #[test]
    fn test() {
        let select_builder = SelectBuilder::from_select_model::<TestDbEntity>();

        let result = select_builder.to_sql_string("Test", NoneWhereModel::new());

        assert_eq!(
            "SELECT id,value #>> '{}' as \"value\" FROM Test",
            result.sql
        );
    }
}
