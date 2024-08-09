use my_postgres::{macros::*, *};
use sql::{SelectBuilder, SqlValues};
use sql_select::SelectEntity;
use sql_where::NoneWhereModel;

#[derive(SelectDbEntity, Debug)]
pub struct MinMaxKeySelectDto {
    #[group_by]
    pub candle_type: i64,
    #[group_by]
    pub instrument_id: String,
    pub is_bid: bool,

    #[db_column_name("date")]
    pub min: GroupByMin<i64>,
    #[db_column_name("date")]
    pub max: GroupByMax<i64>,
}

#[test]
fn test_same_field_min_max() {
    let mut select_builder = SelectBuilder::new();
    MinMaxKeySelectDto::fill_select_fields(&mut select_builder);

    let mut sql = String::new();
    let mut sql_values = SqlValues::new();
    select_builder.build_select_sql(&mut sql, &mut sql_values, "test", NoneWhereModel::new());

    assert_eq!(
        r#"SELECT candle_type,instrument_id,is_bid,MIN(date)::bigint as "min",MAX(date)::bigint as "max" FROM test"#,
        sql
    );
}
