use my_postgres::macros::*;
use serde::*;

#[derive(Serialize, Deserialize, MyPostgresJsonModel, MyPostgresJsonWhereModel)]
pub struct JsonTestField {
    pub key: String,
    pub value: i32,
}

#[derive(WhereDbModel)]
pub struct WhereWithJsonField {
    pub field_before: String,
    #[db_column_name("my_json_field")]
    pub json_field: JsonTestField,
    pub field_after: bool,
}

#[derive(WhereDbModel)]
pub struct WhereWithJsonVecField {
    pub field_before: String,

    pub json_field: Vec<JsonTestField>,
    pub field_after: bool,
}
#[cfg(test)]
mod tests {

    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};

    use super::*;

    #[test]
    fn test_json_as_where_field() {
        let where_model = WhereWithJsonField {
            field_before: "SomeValue".to_string(),
            json_field: JsonTestField {
                key: "key".to_string(),
                value: 5,
            },
            field_after: true,
        };

        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(
            r#"field_before=$1 AND ("my_json_field"->>'key'=$2 AND "my_json_field"->>'value'=5) AND field_after=true"#,
            sql.as_str()
        );
    }

    #[test]
    fn test_json_as_where_vec_field() {
        let where_model = WhereWithJsonVecField {
            field_before: "SomeValue".to_string(),
            json_field: vec![
                JsonTestField {
                    key: "key".to_string(),
                    value: 5,
                },
                JsonTestField {
                    key: "key2".to_string(),
                    value: 6,
                },
            ],
            field_after: true,
        };

        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        println!("{}", sql);
    }
}
