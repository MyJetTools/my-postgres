use std::collections::BTreeMap;

use my_postgres::macros::WhereDbModel;

#[derive(WhereDbModel)]
pub struct WhereWithDynamicJsonValues {
    pub value_before: i32,
    #[db_column_name("my_dynamic_json")]
    pub dynamic_json: BTreeMap<String, String>,
    pub value_after: bool,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};

    use super::*;

    #[test]
    fn test_with_json_dynamic_no_values() {
        let where_model = WhereWithDynamicJsonValues {
            value_before: 1,
            dynamic_json: BTreeMap::new(),
            value_after: true,
        };

        let mut sql = String::new();
        let mut params = SqlValues::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("value_before=1 AND value_after=true", sql);
        assert_eq!(params.len(), 0);
    }

    #[test]
    fn test_with_json_dynamic_single_value() {
        let mut where_model = WhereWithDynamicJsonValues {
            value_before: 1,
            dynamic_json: BTreeMap::new(),
            value_after: true,
        };

        where_model
            .dynamic_json
            .insert("json_field".to_string(), "json_value".to_string());

        let mut sql = String::new();
        let mut params = SqlValues::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(
            "value_before=1 AND \"my_dynamic_json\"->>'json_field'=$1 AND value_after=true",
            sql
        );
        assert_eq!(params.len(), 1);
        assert_eq!(params.get(0).unwrap().as_str().unwrap(), "json_value");
    }

    #[test]
    fn test_with_json_dynamic_several_values() {
        let mut where_model = WhereWithDynamicJsonValues {
            value_before: 1,
            dynamic_json: BTreeMap::new(),
            value_after: true,
        };

        where_model
            .dynamic_json
            .insert("json_field".to_string(), "json_value".to_string());

        where_model
            .dynamic_json
            .insert("json_field2".to_string(), "json_value2".to_string());

        let mut sql = String::new();
        let mut params = SqlValues::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(
            "value_before=1 AND (\"my_dynamic_json\"->>'json_field'=$1 AND \"my_dynamic_json\"->>'json_field2'=$2) AND value_after=true",
            sql
        );
        assert_eq!(params.len(), 2);
        assert_eq!(params.get(0).unwrap().as_str().unwrap(), "json_value");
        assert_eq!(params.get(1).unwrap().as_str().unwrap(), "json_value2");
    }
}
