use my_postgres::macros::WhereDbModel;

#[derive(WhereDbModel)]
pub struct FindTransactionsPsqlWhere {
    #[limit]
    pub limit: usize,
    #[db_column_name("type")]
    pub transaction_type: Vec<i32>,
    #[db_column_name("status")]
    pub transaction_status: Vec<i32>,
    pub wallet_id: Vec<String>,
    pub tx_id: Vec<String>,
}

#[cfg(test)]
mod tests {

    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};

    use super::*;

    #[test]
    fn test_case() {
        let where_model = FindTransactionsPsqlWhere {
            limit: 10,
            transaction_type: vec![],
            transaction_status: vec![],
            wallet_id: vec!["1".to_string(), "2".to_string(), "3".to_string()],
            tx_id: vec![],
        };

        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(sql, "wallet_id IN ($1,$2,$3)");
    }
}
