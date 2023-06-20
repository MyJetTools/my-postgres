use crate::{SqlValue, SqlValueMetadata, SqlWhereValueWriter};

pub struct WhereFieldData<'s> {
    pub field_name: &'s str,
    pub op: Option<&'static str>,
    pub ignore_if_none: bool,
    pub value: Option<&'s dyn SqlWhereValueWriter<'s>>,
    pub meta_data: Option<SqlValueMetadata>,
}
pub trait SqlWhereModel<'s> {
    fn get_where_field_name_data(&'s self, no: usize) -> Option<WhereFieldData<'s>>;

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;

    fn build_where_sql_part(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        include_where: bool,
    ) {
        let mut no = 0;

        let mut rendered_no = 0;

        while let Some(field_data) = self.get_where_field_name_data(no) {
            if field_data.value.is_none() {
                if !field_data.ignore_if_none {
                    if rendered_no > 0 {
                        sql.push_str(" AND ");
                    } else {
                        if include_where {
                            sql.push_str(" WHERE ");
                        }
                    }

                    sql.push_str(field_data.field_name);
                    sql.push_str(" IS NULL")
                }
                no += 1;
                continue;
            }

            let value = field_data.value.unwrap();

            if rendered_no > 0 {
                sql.push_str(" AND ");
            } else {
                sql.push_str(" WHERE ");
            }

            no += 1;
            rendered_no += 1;
            sql.push_str(field_data.field_name);
            if let Some(op) = field_data.op {
                sql.push_str(op);
            } else {
                sql.push_str(value.get_default_operator());
            }
            value.write(sql, params, &field_data.meta_data);
        }
    }

    fn fill_limit_and_offset(&self, sql: &mut String) {
        if let Some(limit) = self.get_limit() {
            sql.push_str(" LIMIT ");
            sql.push_str(limit.to_string().as_str());
        }
        if let Some(offset) = self.get_offset() {
            sql.push_str(" OFFSET ");
            sql.push_str(offset.to_string().as_str());
        }
    }

    fn build_delete_sql(&'s self, table_name: &str) -> (String, Vec<SqlValue<'s>>) {
        let mut sql = String::new();

        sql.push_str("DELETE FROM ");
        sql.push_str(table_name);

        let mut params = Vec::new();
        self.build_where_sql_part(&mut sql, &mut params, true);
        self.fill_limit_and_offset(&mut sql);
        (sql, params)
    }

    fn build_bulk_delete_sql(
        where_models: &'s [impl SqlWhereModel<'s>],
        table_name: &str,
    ) -> (String, Vec<SqlValue<'s>>) {
        if where_models.len() == 1 {
            let where_model = where_models.get(0).unwrap();
            return where_model.build_delete_sql(table_name);
        }
        let mut sql = String::new();

        sql.push_str("DELETE FROM ");
        sql.push_str(table_name);
        sql.push_str(" WHERE ");
        let mut params = Vec::new();
        let mut no = 0;
        for where_model in where_models {
            if no > 0 {
                sql.push_str(" OR ");
            }
            sql.push('(');
            where_model.build_where_sql_part(&mut sql, &mut params, true);
            sql.push(')');

            where_model.fill_limit_and_offset(&mut sql);
            no += 1;
        }

        (sql, params)
    }
}
