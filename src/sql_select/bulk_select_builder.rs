use crate::{sql_where::SqlWhereModel, SqlValue};

pub struct BulkSelectBuilder<'s, TWhereModel: SqlWhereModel<'s>> {
    pub where_models: Vec<TWhereModel>,
    pub table_name: &'s str,
}

impl<'s, TWhereModel: SqlWhereModel<'s>> BulkSelectBuilder<'s, TWhereModel> {
    pub fn new(table_name: &'s str, where_models: Vec<TWhereModel>) -> Self {
        Self {
            table_name,
            where_models,
        }
    }

    pub fn build_sql<TBuildSelect: Fn(&mut String)>(
        &'s self,
        build_select_part: TBuildSelect,
    ) -> (String, Vec<SqlValue<'s>>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        let mut line_no = 0;

        for where_model in &self.where_models {
            if line_no > 0 {
                sql.push_str("UNION ALL\n");
            }

            sql.push_str("SELECT ");
            sql.push_str(line_no.to_string().as_str());
            sql.push_str("::int as line_no, ");
            build_select_part(&mut sql);
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

        (sql, params)
    }
}
