use my_postgres::macros::TableSchema;
use my_postgres::sql::SqlValues;
use my_postgres::sql_where::SqlWhereModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(TableSchema)]
pub struct MyTableModel {
    #[generate_where_model(name:"ByTraderIdAndDateWhereModel")]
    pub trader_id: String,

    #[sql_type("timestamp")]
    #[generate_where_model(name:"ByTraderIdAndDateWhereModel", operator: ">")]
    pub date: DateTimeAsMicroseconds,
}

#[test]
fn test_where_auto_generator_with_operator() {
    let where_model = ByTraderIdAndDateWhereModel {
        trader_id: "test".to_string(),
        date: DateTimeAsMicroseconds::parse_iso_string("2023-06-19T22:07:20.518741+00:00").unwrap(),
    };

    let mut params = SqlValues::new();
    let mut sql = String::new();
    where_model.fill_where_component(&mut sql, &mut params);

    assert_eq!(
        "trader_id=$1 AND date>'2023-06-19T22:07:20.518741+00:00'",
        sql
    );

    assert_eq!("test", params.get(0).unwrap().as_str().unwrap());
}
