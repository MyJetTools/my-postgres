use my_postgres::macros::WhereDbModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(WhereDbModel)]
pub struct FindOlderThanPsqlWhere {
    #[operator("<")]
    #[sql_type("timestamp")]
    #[db_column_name("created_at")]
    pub created_at: DateTimeAsMicroseconds,
    #[limit]
    pub limit: usize,
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::FindOlderThanPsqlWhere;

    #[test]
    fn test() {
        let where_model = FindOlderThanPsqlWhere {
            created_at: DateTimeAsMicroseconds::from_str("2024-05-12T12:34:56.789012").unwrap(),
            limit: 10,
        };

        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        println!("sql: {}", sql);
    }
}
