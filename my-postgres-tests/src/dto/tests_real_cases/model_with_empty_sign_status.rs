use my_postgres::{macros::WhereDbModel, tokio_postgres::types::IsNull};
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(WhereDbModel)]
pub struct WhereByClientIdWithDateRangeAndPaginationModel {
    #[limit]
    pub limit: usize,

    #[offset]
    pub offset: usize,

    #[ignore_if_none]
    pub client_id: Option<String>,

    #[operator(">=")]
    #[db_column_name("created_at")]
    #[sql_type("timestamp")]
    #[ignore_if_none]
    pub from_date: Option<DateTimeAsMicroseconds>,

    #[operator("<=")]
    #[db_column_name("created_at")]
    #[sql_type("timestamp")]
    #[ignore_if_none]
    pub to_date: Option<DateTimeAsMicroseconds>,

    pub sign_status: IsNull,
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel, tokio_postgres::types::IsNull};
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::WhereByClientIdWithDateRangeAndPaginationModel;

    #[test]
    fn test() {
        let where_model = WhereByClientIdWithDateRangeAndPaginationModel {
            limit: 10,
            offset: 0,
            client_id: None,
            from_date: Some(DateTimeAsMicroseconds::from_str("2021-01-01").unwrap()),
            to_date: Some(DateTimeAsMicroseconds::from_str("2021-01-02").unwrap()),
            sign_status: IsNull::Yes,
        };

        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("created_at>='2021-01-01T00:00:00+00:00' AND created_at<='2021-01-02T00:00:00+00:00' AND sign_status IS NULL", sql);
    }
}
