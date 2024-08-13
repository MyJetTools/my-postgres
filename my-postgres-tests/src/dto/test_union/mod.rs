use my_postgres::{
    macros::{SelectDbEntity, WhereDbModel},
    sql::SqlValues,
};

#[derive(WhereDbModel, Debug)]
pub struct WhereModel {
    pub trader_id: String,

    #[limit]
    pub limit: usize,
}

#[derive(SelectDbEntity, Debug)]
pub struct SelectTestModel {
    #[allow(dead_code)]
    pub trader_id: String,
}

#[test]
fn test_bulk_sql_forming() {
    let mut sql = String::new();

    let mut sql_values = SqlValues::new();

    let where_models = vec![
        WhereModel {
            trader_id: "1".to_string(),
            limit: 10,
        },
        WhereModel {
            trader_id: "2".to_string(),
            limit: 20,
        },
    ];

    my_postgres::union::compile_union_select::<SelectTestModel, WhereModel>(
        &mut sql,
        &mut sql_values,
        "TEST",
        &where_models,
    );

    println!("sql: {}", sql);
}
