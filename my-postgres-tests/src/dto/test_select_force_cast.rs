use my_postgres::macros::SelectDbEntity;

#[derive(Debug, SelectDbEntity)]
pub struct ForceCast {
    #[force_cast_to_db_type]
    pub value: String,
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

        assert_eq!("value::text", sql);
    }
}
