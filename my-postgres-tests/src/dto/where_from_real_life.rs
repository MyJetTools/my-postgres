use my_postgres::macros::WhereDbModel;

#[derive(WhereDbModel)]
pub struct WhereByIdWithPaginationModel<'s> {
    pub client_id: &'s str,
    #[limit]
    pub limit: usize,
    #[offset]
    pub offset: usize,
    #[ignore_if_none]
    pub status: Option<Vec<String>>,
}

#[cfg(test)]
mod test {
    use my_postgres::sql_where::SqlWhereModel;

    #[test]
    fn test() {
        let where_model = super::WhereByIdWithPaginationModel {
            client_id: "client_id",
            limit: 10,
            offset: 0,
            status: None,
        };

        assert_eq!(where_model.has_conditions(), true);
    }
}
