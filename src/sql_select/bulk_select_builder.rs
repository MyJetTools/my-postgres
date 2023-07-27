use crate::{
    sql::{SelectBuilder, SqlData, SqlValues},
    sql_where::SqlWhereModel,
};

use super::SelectEntity;

pub struct BulkSelectBuilder<TWhereModel: SqlWhereModel> {
    pub where_models: Vec<TWhereModel>,
    pub table_name: &'static str,
}

impl<TWhereModel: SqlWhereModel> BulkSelectBuilder<TWhereModel> {
    pub fn new(table_name: &'static str, where_models: Vec<TWhereModel>) -> Self {
        Self {
            table_name,
            where_models,
        }
    }

    pub fn build_sql<TSelectEntity: SelectEntity>(&self) -> SqlData {
        let mut sql = String::new();
        let mut params = SqlValues::new();

        let mut line_no = 0;

        for where_model in &self.where_models {
            if line_no > 0 {
                sql.push_str("UNION ALL\n");
            }

            let mut select_builder = SelectBuilder::new();
            select_builder.push(crate::sql::SelectFieldValue::LineNo(line_no));

            TSelectEntity::fill_select_fields(&mut select_builder);

            select_builder.build_select_sql(
                &mut sql,
                &mut params,
                self.table_name,
                Some(where_model),
            );

            sql.push('\n');
            line_no += 1;
        }

        SqlData::new(sql, params)
    }
}
