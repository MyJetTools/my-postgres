use crate::{sql::SqlValues, sql_where::SqlWhereModel};

use super::SqlUpdateModelValue;

pub trait SqlUpdateModel<'s> {
    fn get_column_name(no: usize) -> (&'static str, Option<&'static str>);
    fn get_field_value(&'s self, no: usize) -> SqlUpdateModelValue<'s>;
    fn get_fields_amount() -> usize;

    fn get_e_tag_column_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);

    fn fill_update_columns(sql: &mut String) {
        let amount = Self::get_fields_amount();

        if amount == 1 {
            let (column_name, related_column_name) = Self::get_column_name(0);

            match related_column_name {
                Some(related_column_name) => {
                    sql.push('(');
                    sql.push_str(column_name);
                    sql.push(',');
                    sql.push_str(related_column_name);
                    sql.push(')');
                }
                None => {
                    sql.push_str(column_name);
                }
            }

            return;
        }

        sql.push('(');

        let mut has_first_column = false;
        for no in 0..amount {
            let (column_name, related_column_name) = Self::get_column_name(no);

            if has_first_column {
                sql.push(',');
            } else {
                has_first_column = true;
            }

            sql.push_str(column_name);

            if let Some(related_column_name) = related_column_name {
                sql.push(',');
                sql.push_str(related_column_name);
            }
        }
        sql.push('(');
    }

    fn build_update_sql_part(&'s self, sql: &mut String, params: &mut SqlValues<'s>) {
        Self::fill_update_columns(sql);
        sql.push('=');
        for i in 0..Self::get_fields_amount() {
            if i > 0 {
                sql.push(',');
            }

            let update_data = self.get_field_value(i);

            match &update_data.value {
                Some(value) => {
                    let value = value.get_update_value(params, &update_data.metadata);
                    value.write(sql);
                }
                None => {
                    let (_, related_column_name) = Self::get_column_name(i);
                    if related_column_name.is_some() {
                        sql.push_str("NULL");
                    } else {
                        sql.push_str("NULL,NULL");
                    }
                }
            }
        }
    }

    fn fill_upsert_sql_part(sql: &mut String) {
        for i in 0..Self::get_fields_amount() {
            if i > 0 {
                sql.push(',');
            }
            let (column_name, related_name) = Self::get_column_name(i);

            sql.push_str(column_name);
            sql.push_str("=EXCLUDED.");
            sql.push_str(column_name);

            if let Some(additional_name) = related_name {
                sql.push(',');
                sql.push_str(additional_name);
                sql.push_str("=EXCLUDED.");
                sql.push_str(additional_name);
            }
        }
    }

    fn build_update_sql(
        &'s self,
        table_name: &str,
        where_model: Option<&'s impl SqlWhereModel<'s>>,
    ) -> (String, SqlValues<'s>) {
        let mut result = String::new();

        result.push_str("UPDATE ");
        result.push_str(table_name);
        result.push_str(" SET ");

        let mut params = SqlValues::new();

        self.build_update_sql_part(&mut result, &mut params);

        if let Some(where_model) = where_model {
            let where_builder = where_model.build_where_sql_part(&mut params);

            if where_builder.has_conditions() {
                result.push_str(" WHERE ");
                where_builder.build(&mut result);
            }

            where_model.fill_limit_and_offset(&mut result);
        }

        //crate::sql_where::build(&mut result, where_model, &mut params);

        (result, params)
    }
}
