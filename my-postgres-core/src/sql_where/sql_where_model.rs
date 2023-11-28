use crate::{
    sql::{SqlData, SqlValues, WhereBuilder, WhereBuilderFromFields},
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

const NULL_VALUE: &'static str = "NULL";
pub trait SqlWhereModel {
    fn get_where_field_name_data<'s>(&'s self, no: usize) -> Option<WhereFieldData<'s>>;

    fn get_limit(&self) -> Option<usize>;
    fn get_offset(&self) -> Option<usize>;

    fn build_where_sql_part(&self, params: &mut crate::sql::SqlValues) -> WhereBuilder {
        let mut no = 0;

        let mut result = WhereBuilderFromFields::new();

        while let Some(field_data) = self.get_where_field_name_data(no) {
            match field_data {
                WhereFieldData::Data {
                    column_name,
                    op,
                    ignore_if_none,
                    value,
                    meta_data,
                } => {
                    match value {
                        Some(value) => {
                            let where_value = value.get_where_value(params, &meta_data);

                            let op = if let Some(op) = op {
                                op
                            } else {
                                value.get_default_operator()
                            };

                            result.push_where_condition(column_name, op, where_value);
                        }
                        None => {
                            if !ignore_if_none {
                                result.push_where_condition(
                                    column_name,
                                    " IS ",
                                    crate::sql::SqlWhereValue::NonStringValue(NULL_VALUE.into()),
                                );
                            }
                        }
                    }

                    no += 1;
                }
                WhereFieldData::Raw(data) => {
                    let mut sql = String::new();
                    for itm in &data {
                        match itm {
                            WhereRawData::Content(content) => {
                                sql.push_str(content);
                            }
                            WhereRawData::PlaceHolder(value) => {
                                let where_value = value.get_where_value(params, &None);
                                where_value.push_value(&mut sql);
                            }
                        }
                    }

                    return WhereBuilder::Raw(sql);
                }
            }
        }

        WhereBuilder::Fields(result)
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

    fn build_delete_sql(&self, table_name: &str) -> SqlData {
        let mut sql = String::new();

        sql.push_str("DELETE FROM ");
        sql.push_str(table_name);

        let mut params = SqlValues::new();

        let where_builder = self.build_where_sql_part(&mut params);

        if where_builder.has_conditions() {
            sql.push_str(" WHERE ");
            where_builder.build(&mut sql);
        }

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

        SqlData::new(sql, params)
    }
}
