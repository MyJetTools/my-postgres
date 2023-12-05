use my_postgres::macros::WhereDbModel;

#[derive(WhereDbModel)]
pub struct WhereModelWithStr<'a> {
    pub field_name: &'a str,
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};

    use super::WhereModelWithStr;

    #[test]
    fn test_generating_where_part() {
        let where_model = WhereModelWithStr { field_name: "test" };
        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("field_name=$1", sql);
    }
}
