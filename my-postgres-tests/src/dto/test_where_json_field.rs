use my_postgres::macros::WhereDbModel;

#[derive(Debug, WhereDbModel)]
pub struct WhereJsonField {
    #[inside_json("db_column_name.sub_field")]
    pub json_prop: String,
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_inside_json() {
        use my_postgres::sql_where::SqlWhereModel;

        use super::WhereJsonField;

        let where_model = WhereJsonField {
            json_prop: "test".to_string(),
        };

        let mut params = my_postgres::sql::SqlValues::new();
        let mut sql = String::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("db_column_name->>sub_field->>json_prop=$1", sql);
    }
}
