use my_postgres::macros::TableSchema;
use my_postgres::sql::SqlValues;
use my_postgres::sql_where::SqlWhereModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(TableSchema)]
pub struct MyTableModel {
    #[generate_select_model("MySelectDto")]
    #[generate_where_model(name:"ByTraderIdAndDateWhereModel", as_str)]
    #[db_column_name(name:"my_trader_id")]
    pub trader_id: String,

    #[sql_type("timestamp")]
    #[generate_where_model(name:"ByTraderIdAndDateWhereModel", operator_from: ">", operator_to: "<")]
    pub date: DateTimeAsMicroseconds,
}

#[test]
fn test_where_auto_generator_with_operator() {
    let where_model = ByTraderIdAndDateWhereModel {
        trader_id: "test",
        date_from: DateTimeAsMicroseconds::parse_iso_string("2023-06-19T22:07:20.518741+00:00")
            .unwrap(),
        date_to: DateTimeAsMicroseconds::parse_iso_string("2023-06-19T22:07:20.518741+00:00")
            .unwrap(),
    };

    let mut sql = String::new();
    let mut params = SqlValues::new();
    where_model.fill_where_component(&mut sql, &mut params);

    assert_eq!("my_trader_id=$1 AND date>'2023-06-19T22:07:20.518741+00:00' AND date<'2023-06-19T22:07:20.518741+00:00'", sql);

    assert_eq!("test", params.get(0).unwrap().as_str().unwrap());
}
