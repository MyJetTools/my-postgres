use my_postgres::macros::where_raw_model;

#[where_raw_model("Content=${field_1} AND Content2=${field_2} AND Content3 in ${field_3}")]
pub struct WhereRawModel {
    pub field_1: String,
    pub field_2: bool,
    pub field_3: Vec<i32>,
}

#[cfg(test)]
mod tests {
    use my_postgres::sql_where::SqlWhereModel;

    use super::WhereRawModel;

    #[test]
    fn test_raw_model() {
        let where_model = WhereRawModel {
            field_1: "test".to_string(),
            field_2: true,
            field_3: vec![1, 2, 3],
        };

        let mut params = my_postgres::sql::SqlValues::new();
        let mut sql = String::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(
            "Content=$1 AND Content2=true AND Content3 in (1,2,3)",
            sql.as_str()
        );
    }

    #[test]
    fn test_raw_model_with_empty_vec() {
        let where_model = WhereRawModel {
            field_1: "test".to_string(),
            field_2: true,
            field_3: vec![],
        };

        let mut params = my_postgres::sql::SqlValues::new();
        let mut sql = String::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("Content=$1 AND Content2=true", sql.as_str());
    }

    #[test]
    fn test_raw_model_with_vec_single_value() {
        let where_model = WhereRawModel {
            field_1: "test".to_string(),
            field_2: true,
            field_3: vec![1],
        };

        let mut params = my_postgres::sql::SqlValues::new();
        let mut sql = String::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(
            "Content=$1 AND Content2=true AND Content3 in (1)",
            sql.as_str()
        );
    }
}
