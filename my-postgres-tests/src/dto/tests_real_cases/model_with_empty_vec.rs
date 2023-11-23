use my_postgres::macros::WhereDbModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(WhereDbModel)]
pub struct GetSessionsPagePsql {
    pub trader_id: String,
    #[operator("<")]
    #[sql_type("timestamp")]
    pub created_date: DateTimeAsMicroseconds,
    pub action_type: Vec<i32>,
    #[limit]
    pub limit: usize,
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::GetSessionsPagePsql;

    #[test]
    fn test_empty_action_type() {
        let where_model = GetSessionsPagePsql {
            trader_id: "id".to_string(),
            created_date: DateTimeAsMicroseconds::new(0),
            action_type: vec![],
            limit: 15,
        };

        let mut params = SqlValues::new();
        let where_builder: my_postgres::sql::WhereBuilder =
            where_model.build_where_sql_part(&mut params);

        let mut sql = String::new();

        where_builder.build(&mut sql);

        println!("{}", sql);

        assert_eq!(
            sql,
            "trader_id=$1 AND created_date<'1970-01-01T00:00:00+00:00'"
        );
    }
}
