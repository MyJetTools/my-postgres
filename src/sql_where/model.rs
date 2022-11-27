pub trait SqlWhereModel<'s> {
    fn fill_where(
        &'s self,
        sql: &mut String,
        params: Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
    ) -> Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>;

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;
}
