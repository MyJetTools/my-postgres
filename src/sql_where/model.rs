use crate::{sql::WhereBuilder, SqlValue, SqlValueMetadata, SqlWhereValueProvider};

pub struct WhereFieldData<'s> {
    pub field_name: &'static str,
    pub op: Option<&'static str>,
    pub ignore_if_none: bool,
    pub value: Option<&'s dyn SqlWhereValueProvider<'s>>,
    pub meta_data: Option<SqlValueMetadata>,
}

const NULL_VALUE: &'static str = "NULL";
pub trait SqlWhereModel<'s> {
    fn get_where_field_name_data(&'s self, no: usize) -> Option<WhereFieldData<'s>>;

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;

    fn build_where_sql_part(&'s self, params: &mut Vec<SqlValue<'s>>) -> WhereBuilder {
        let mut no = 0;

        let mut result = WhereBuilder::new();

        while let Some(field_data) = self.get_where_field_name_data(no) {
            match field_data.value {
                Some(value) => {
                    let where_value = value.get_where_value(params, &field_data.meta_data);

                    let op = if let Some(op) = field_data.op {
                        op
                    } else {
                        value.get_default_operator()
                    };

                    result.push_where_condition(field_data.field_name, op, where_value);
                }
                None => {
                    if !field_data.ignore_if_none {
                        result.push_where_condition(
                            field_data.field_name,
                            " IS ",
                            crate::sql::SqlWhereValue::NonStringValue(NULL_VALUE.into()),
                        );
                    }
                }
            }

            no += 1;
        }

        result
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

        let where_builder = self.build_where_sql_part(&mut params);

        where_builder.build(&mut sql);

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
            let where_builder = where_model.build_where_sql_part(&mut params);

            if where_builder.has_conditions() {
                if no > 0 {
                    sql.push_str(" OR ");
                }

                sql.push('(');

                where_builder.build(&mut sql);
                sql.push(')');

                where_model.fill_limit_and_offset(&mut sql);
                no += 1;
            }
        }

        (sql, params)
    }
}
