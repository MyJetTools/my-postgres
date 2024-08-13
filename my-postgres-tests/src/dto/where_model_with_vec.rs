use my_postgres::{macros::WhereDbModel, sql_where::SqlWhereModel};

#[derive(Debug, WhereDbModel)]
pub struct WhereModel {
    pub data: Vec<String>,
}

#[test]
fn test_has_no_conditions_with_empty_vec() {
    let where_model = WhereModel { data: Vec::new() };
    assert_eq!(false, where_model.has_conditions());

    let where_model = WhereModel {
        data: vec!["Test".to_string()],
    };
    assert_eq!(true, where_model.has_conditions());
}
