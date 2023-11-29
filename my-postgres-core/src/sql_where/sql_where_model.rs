use crate::{
    sql::{SqlData, SqlValues},
    ColumnName, SqlValueMetadata, SqlWhereValueProvider,
};

pub enum WhereRawData<'s> {
    Content(&'static str),
    PlaceHolder(&'s dyn SqlWhereValueProvider),
}

pub enum WhereFieldData<'s> {
    Data {
        column_name: ColumnName,
        op: Option<&'static str>,
        ignore_if_none: bool,
        value: Option<&'s dyn SqlWhereValueProvider>,
        meta_data: Option<SqlValueMetadata>,
    },
    Raw(Vec<WhereRawData<'s>>),
}

pub trait SqlWhereModel {
    fn fill_where_component(&self, sql: &mut String, params: &mut SqlValues);

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;

    fn has_conditions(&self) -> bool;

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

    fn build_delete_sql(&self, table_name: &str) -> SqlData {
        let mut sql = String::new();

        sql.push_str("DELETE FROM ");
        sql.push_str(table_name);

        let mut params = SqlValues::new();

        if self.has_conditions() {
            sql.push_str(" WHERE ");
        }

        self.fill_where_component(&mut sql, &mut params);

        self.fill_limit_and_offset(&mut sql);
        SqlData::new(sql, params)
    }

    fn build_bulk_delete_sql(where_models: &[impl SqlWhereModel], table_name: &str) -> SqlData {
        if where_models.len() == 1 {
            let where_model = where_models.get(0).unwrap();
            return where_model.build_delete_sql(table_name);
        }
        let mut sql = String::new();

        sql.push_str("DELETE FROM ");
        sql.push_str(table_name);
        sql.push_str(" WHERE ");
        let mut params = SqlValues::new();
        let mut no = 0;
        for where_model in where_models {
            if where_model.has_conditions() {
                if no > 0 {
                    sql.push_str(" OR ");
                }

                sql.push('(');
                where_model.fill_where_component(&mut sql, &mut params);
                sql.push(')');

                where_model.fill_limit_and_offset(&mut sql);
                no += 1;
            }
        }

        SqlData::new(sql, params)
    }
}
