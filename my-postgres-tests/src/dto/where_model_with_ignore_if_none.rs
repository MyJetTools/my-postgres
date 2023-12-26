use my_postgres::macros::WhereDbModel;

#[derive(WhereDbModel)]
struct TestWhereModel {
    #[ignore_if_none]
    pub client_id: Option<String>,

    #[ignore_if_none]
    pub trader_account_id: Option<String>,

    #[limit]
    pub limit: usize,

    #[offset]
    pub offset: usize,
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};

    use super::TestWhereModel;

    #[test]
    fn test_where_model() {
        let where_model = TestWhereModel {
            client_id: None,
            trader_account_id: None,
            limit: 0,
            offset: 0,
        };
        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("", sql);
    }
}
