use my_postgres::macros::*;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(SelectDbEntity, InsertDbEntity, UpdateDbEntity, TableSchema)]
pub struct DateTimeAsBigintDto {
    #[primary_key(0)]
    pub id: i32,

    #[sql_type("bigint")]
    pub moment: DateTimeAsMicroseconds,

    #[sql_type("bigint")]
    pub opt_moment: Option<DateTimeAsMicroseconds>,
}

#[derive(SelectDbEntity)]
pub struct DateTimeAsTimestampDto {
    #[sql_type("timestamp")]
    pub moment: DateTimeAsMicroseconds,

    #[sql_type("timestamp")]
    pub opt_moment: Option<DateTimeAsMicroseconds>,
}

#[derive(BulkSelectDbEntity, SelectDbEntity)]
pub struct BulkDateTimeAsBigintDto {
    pub line_no: i32,
    pub id: i32,
    #[sql_type("bigint")]
    pub moment: DateTimeAsMicroseconds,
}

#[cfg(test)]
mod tests {

    use my_postgres::macros::WhereDbModel;
    use my_postgres::sql::SelectBuilder;
    use my_postgres::sql_select::BulkSelectBuilder;

    use super::*;

    #[derive(WhereDbModel)]
    pub struct WhereById {
        pub id: i32,
    }

    /// The name FromDbRow<DateTimeAsMicroseconds> reads out of the row.
    fn column_name_the_reader_asks_for(db_column_name: &str) -> String {
        let mut result = String::new();
        my_postgres::utils::fill_adjusted_column_name(db_column_name, &mut result);
        result
    }

    #[test]
    fn test_select_date_time_as_bigint() {
        let select_builder = SelectBuilder::from_select_model::<DateTimeAsBigintDto>();

        let mut sql = String::new();
        select_builder.fill_select_fields(&mut sql);

        assert_eq!(
            r#"id,moment as "moment.transformed",opt_moment as "opt_moment.transformed""#,
            sql
        );
    }

    #[test]
    fn test_select_date_time_as_timestamp() {
        let select_builder = SelectBuilder::from_select_model::<DateTimeAsTimestampDto>();

        let mut sql = String::new();
        select_builder.fill_select_fields(&mut sql);

        assert_eq!(
            r#"(extract(EPOCH FROM moment) * 1000000)::bigint as "moment.transformed",(extract(EPOCH FROM opt_moment) * 1000000)::bigint as "opt_moment.transformed""#,
            sql
        );
    }

    #[test]
    fn test_bulk_select_date_time_as_bigint() {
        let builder = BulkSelectBuilder::new("test", vec![WhereById { id: 1 }]);

        let sql = builder.build_sql::<BulkDateTimeAsBigintDto>();

        assert_eq!(
            "SELECT 0::int as \"line_no\",id,moment as \"moment.transformed\" FROM test WHERE id=1\n",
            sql.sql
        );
    }

    /// The bug this guards against: the select builder emitted the bigint column with no alias,
    /// while the row reader always asks for the adjusted name - so every read panicked with
    /// "invalid column `moment.transformed`". Neither side fails to compile and the SQL itself
    /// is valid, so only comparing the two names catches the drift.
    #[test]
    fn test_date_time_alias_matches_what_the_reader_asks_for() {
        for select_model_sql in [
            {
                let mut sql = String::new();
                SelectBuilder::from_select_model::<DateTimeAsBigintDto>().fill_select_fields(&mut sql);
                sql
            },
            {
                let mut sql = String::new();
                SelectBuilder::from_select_model::<DateTimeAsTimestampDto>()
                    .fill_select_fields(&mut sql);
                sql
            },
        ] {
            for db_column_name in ["moment", "opt_moment"] {
                let expected = format!(
                    r#" as "{}""#,
                    column_name_the_reader_asks_for(db_column_name)
                );

                assert!(
                    select_model_sql.contains(expected.as_str()),
                    "sql '{}' does not alias column '{}' as '{}'",
                    select_model_sql,
                    db_column_name,
                    column_name_the_reader_asks_for(db_column_name)
                );
            }
        }
    }
}
