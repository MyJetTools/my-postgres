use crate::{
    sql::{SqlValues, UsedColumns},
    ColumnName,
};

use super::SqlUpdateModelValue;

pub trait SqlUpdateModel {
    fn get_column_name(no: usize) -> ColumnName;
    fn get_field_value(&self, no: usize) -> SqlUpdateModelValue;
    fn get_fields_amount() -> usize;

    fn fill_update_columns(sql: &mut String) {
        let amount = Self::get_fields_amount();

        if amount == 1 {
            let column_name = Self::get_column_name(0);
            column_name.push_name(sql);

            return;
        }

        sql.push('(');

        let mut has_first_column = false;
        for no in 0..amount {
            let column_name = Self::get_column_name(no);

            if has_first_column {
                sql.push(',');
            } else {
                has_first_column = true;
            }

            column_name.push_name(sql);
        }
        sql.push(')');
    }

    fn fill_update_values(&self, sql: &mut String, params: &mut SqlValues) {
        let fields_amount = Self::get_fields_amount();

        let need_parentheses = fields_amount > 1;

        if need_parentheses {
            sql.push('(');
        }

        for i in 0..fields_amount {
            if i > 0 {
                sql.push(',');
            }

            let update_data = self.get_field_value(i);
            update_data.write_value(sql, params);
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

    fn fill_upsert_sql_part(sql: &mut String, columns: &UsedColumns) {
        let mut i = 0;
        for no in 0..Self::get_fields_amount() {
            let column_name = Self::get_column_name(no);

            if columns.has_column(&column_name) {
                if i > 0 {
                    sql.push(',');
                }

                i += 1;
                column_name.push_name(sql);

                sql.push_str("=EXCLUDED.");
                column_name.push_name(sql);
            }
        }
    }
}
