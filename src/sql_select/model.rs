use crate::sql_where::SqlWhereModel;

use super::BulkSelectBuilder;

pub trait BulkSelectEntity {
    fn get_line_no(&self) -> i32;
}

pub trait SelectEntity {
    fn from(row: &tokio_postgres::Row) -> Self;
    fn fill_select_fields(sql: &mut String);
    fn get_order_by_fields() -> Option<&'static str>;
    fn get_group_by_fields() -> Option<&'static str>;

    fn build_select_sql<'s, TWhereModel: SqlWhereModel<'s>>(
        table_name: &str,
        where_model: Option<&'s TWhereModel>,
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        sql.push_str("SELECT ");
        Self::fill_select_fields(&mut sql);
        sql.push_str(" FROM ");
        sql.push_str(table_name);

        if let Some(where_model) = where_model {
            where_model.build_where_sql_part(&mut sql, &mut params, true);
        }

        if let Some(order_by_fields) = Self::get_group_by_fields() {
            sql.push_str(order_by_fields);
        }

        if let Some(group_by_fields) = Self::get_group_by_fields() {
            sql.push_str(group_by_fields);
        }

        if let Some(where_model) = where_model {
            where_model.fill_limit_and_offset(&mut sql);
        }

        (sql, params)
    }

    fn build_bulk_select<'s, TWhereModel: SqlWhereModel<'s>>(
        table_name: &'static str,
        where_models: Vec<TWhereModel>,
    ) -> BulkSelectBuilder<'s, TWhereModel> {
        BulkSelectBuilder::new(table_name, where_models)
    }
}
