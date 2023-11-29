use my_postgres::macros::WhereDbModel;

#[derive(WhereDbModel)]
pub struct WhereByIdModel<'s> {
    pub id: &'s str,
}

#[cfg(test)]
mod test {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};

    use super::WhereByIdModel;

    #[test]
    fn test() {
        let where_model = WhereByIdModel { id: "test" };

        let mut sql = String::new();
        let mut params = SqlValues::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("id=$1", sql);

        assert_eq!(params.get(0).unwrap().as_str().unwrap(), "test")
    }
}
