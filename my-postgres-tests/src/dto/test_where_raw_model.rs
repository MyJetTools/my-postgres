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
        let where_builder = where_model.build_where_sql_part(&mut params);

        let mut sql = String::new();

        where_builder.build(&mut sql);

        println!("sql: {}", sql);
    }
}