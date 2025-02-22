use my_postgres::macros::*;
use serde::{Deserialize, Serialize};

#[derive(InsertDbEntity, SelectDbEntity, TableSchema)]
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
    use my_postgres::{
        sql::{SelectBuilder, UsedColumns},
        sql_where::NoneWhereModel,
    };

    use crate::dto::enum_generator_tests::test_enum_as_string_with_model::{
        BankTransferDetailsDto, PaymentDetailsDto, TestDbEntity,
    };

    #[test]
    fn test_select() {
        let select_builder = SelectBuilder::from_select_model::<TestDbEntity>();

        let result = select_builder.to_sql_string("Test", NoneWhereModel::new());

        assert_eq!(
            "SELECT id,value #>> '{}' as \"value.transformed\" FROM Test",
            result.sql
        );
    }

    #[test]
    fn test_insert() {
        let model = TestDbEntity {
            id: 15,
            value: PaymentDetailsDto::BankTransfer(BankTransferDetailsDto {
                bank_name: "test".to_string(),
            }),
        };

        let result =
            my_postgres::sql::build_insert_sql(&model, "TEST", &mut UsedColumns::as_none());

        assert_eq!(
            "INSERT INTO TEST(id,value) VALUES (15,cast($1::text as json))",
            result.sql
        );
    }
}
