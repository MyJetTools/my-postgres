use crate::sql_where::SqlWhereModel;

use super::SqlUpdateValue;

pub trait SqlUpdateModel<'s> {
    fn get_column_name(no: usize) -> (&'static str, Option<&'static str>);
    fn get_field_value(&'s self, no: usize) -> (SqlUpdateValue<'s>, Option<SqlUpdateValue>);
    fn get_fields_amount() -> usize;

    fn get_e_tag_column_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);

    fn build_update_sql_part(&'s self, sql: &mut String, params: &mut Vec<crate::SqlValue<'s>>) {
        for i in 0..Self::get_fields_amount() {
            if i > 0 {
                sql.push(',');
            }

            let (column_name, related_column_name) = Self::get_column_name(i);
            let (update_data, update_related_data) = self.get_field_value(i);

            sql.push_str(column_name);
            sql.push_str("=");

            match &update_data.value {
                Some(value) => {
                    value.write(sql, params, &update_data.metadata);
                }
                None => {
                    sql.push_str("NULL");
                }
            }

            if let Some(additional_column_name) = related_column_name {
                if let Some(update_related_data) = update_related_data {
                    sql.push(',');
                    sql.push_str(additional_column_name);
                    sql.push_str("=");

                    match &update_related_data.value {
                        Some(value) => {
                            value.write(sql, params, &update_related_data.metadata);
                        }
                        None => {
                            sql.push_str("NULL");
                        }
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
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let mut result = String::new();

        result.push_str("UPDATE ");
        result.push_str(table_name);
        result.push_str(" SET ");

        let mut params = Vec::new();

        self.build_update_sql_part(&mut result, &mut params);

        if let Some(where_model) = where_model {
            where_model.build_where_sql_part(&mut result, &mut params, true);

            where_model.fill_limit_and_offset(&mut result);
        }

        //crate::sql_where::build(&mut result, where_model, &mut params);

        (result, params)
    }
}
