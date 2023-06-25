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
            let mut select_builder = SelectBuilder::new();
            if line_no > 0 {
                sql.push_str("UNION ALL\n");
            }

            sql.push_str("SELECT ");

            select_builder.push(crate::sql::SelectFieldValue::LineNo(line_no));

            TSelectEntity::fill_select_fields(&mut select_builder);

            sql.push_str(" FROM ");
            sql.push_str(self.table_name);

            let where_condition = where_model.build_where_sql_part(&mut params);

            if where_condition.has_conditions() {
                sql.push_str(" WHERE ");
                where_condition.build(&mut sql);
            }

            where_model.fill_limit_and_offset(&mut sql);

            sql.push('\n');
            line_no += 1;
        }

        SqlData::new(sql, params)
    }
}
