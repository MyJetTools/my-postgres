use crate::{
    sql::{SqlValues, UpsertColumns},
    ColumnName,
};

use super::SqlUpdateModelValue;

pub trait SqlUpdateModel {
    fn get_column_name(no: usize) -> (ColumnName, Option<ColumnName>);
    fn get_field_value(&self, no: usize) -> SqlUpdateModelValue;
    fn get_fields_amount() -> usize;

    fn fill_update_columns(sql: &mut String) {
        let amount = Self::get_fields_amount();

        if amount == 1 {
            let (column_name, related_column_name) = Self::get_column_name(0);

            match related_column_name {
                Some(related_column_name) => {
                    sql.push('(');
                    column_name.push_name(sql);

                    sql.push(',');
                    related_column_name.push_name(sql);
                    sql.push(')');
                }
                None => {
                    column_name.push_name(sql);
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

            column_name.push_name(sql);

            if let Some(related_column_name) = related_column_name {
                sql.push(',');
                related_column_name.push_name(sql);
            }
        }
        sql.push(')');
    }

    fn fill_update_values(&self, sql: &mut String, params: &mut SqlValues) {
        let fields_amount = Self::get_fields_amount();

        let need_parentheses = if fields_amount == 1 {
            let columns = Self::get_column_name(0);
            columns.1.is_some()
        } else {
            true
        };

        if need_parentheses {
            sql.push('(');
        }

        for i in 0..fields_amount {
            if i > 0 {
                sql.push(',');
            }

            let update_data = self.get_field_value(i);
            update_data.write_value(sql, params, || Self::get_column_name(i));
        }

        if need_parentheses {
            sql.push(')');
        }
    }

    fn build_update_sql_part(&self, sql: &mut String, params: &mut SqlValues) {
        Self::fill_update_columns(sql);
        sql.push('=');
        self.fill_update_values(sql, params);
    }

    fn fill_upsert_sql_part(sql: &mut String, columns: &UpsertColumns) {
        for i in 0..Self::get_fields_amount() {
            if i > 0 {
                sql.push(',');
            }
            let (column_name, related_name) = Self::get_column_name(i);

            if columns.has_column(&column_name) {
                column_name.push_name(sql);

                sql.push_str("=EXCLUDED.");
                column_name.push_name(sql);
            }

            if let Some(additional_name) = related_name {
                if columns.has_column(&additional_name) {
                    sql.push(',');
                    additional_name.push_name(sql);
                    sql.push_str("=EXCLUDED.");
                    additional_name.push_name(sql);
                }
            }
        }
    }

    /*
    fn build_update_sql(
        &self,
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

        (result, params)
    }
     */
}
