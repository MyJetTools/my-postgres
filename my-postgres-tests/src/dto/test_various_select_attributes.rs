use my_postgres::macros::SelectDbEntity;

#[derive(Debug, SelectDbEntity)]
pub struct ForceCast {
    #[force_cast_db_type]
    pub value: String,

    #[wrap_column_name("pg_size_pretty(pg_database_size(test)) as ${}")]
    pub value2: String,
}

#[cfg(test)]
mod tests {
    use my_postgres::sql::SelectBuilder;

    use super::ForceCast;

    #[test]
    fn test() {
        let select_builder = SelectBuilder::from_select_model::<ForceCast>();

        let mut sql = String::new();

        select_builder.fill_select_fields(&mut sql);

        assert_eq!(
            "value::text,pg_size_pretty(pg_database_size(test)) as value2",
            sql
        );
    }
}
