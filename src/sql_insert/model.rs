use std::collections::HashMap;

use crate::{sql_update::SqlUpdateModel, SqlUpdateValueWrapper};

pub trait SqlInsertModel<'s> {
    fn get_fields_amount() -> usize;
    fn get_field_name(no: usize) -> &'static str;
    fn get_field_value(&'s self, no: usize) -> SqlUpdateValueWrapper<'s>;

    fn get_e_tag_column_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);

    fn generate_insert_fields(
        &'s self,
        sql: &mut String,
        params: &mut Vec<crate::SqlValue<'s>>,
        params_with_index: &mut Option<std::collections::HashMap<&'static str, usize>>,
    ) {
        sql.push_str(" (");

        let mut no = 0;

        let mut values = Vec::new();

        for field_no in 0..Self::get_fields_amount() {
            let sql_value = self.get_field_value(field_no);

            match &sql_value {
                SqlUpdateValueWrapper::Ignore => {}
                SqlUpdateValueWrapper::Value {
                    value: _,
                    metadata: _,
                } => {
                    if no > 0 {
                        sql.push(',');
                    }
                    no += 1;
                    let field_name = Self::get_field_name(field_no);
                    sql.push_str(field_name);
                    values.push((field_name, sql_value));
                }
                SqlUpdateValueWrapper::Null => {}
            }
        }
        sql.push_str(") VALUES (");
        no = 0;
        for (field_name, sql_value) in values {
            match sql_value {
                SqlUpdateValueWrapper::Ignore => {}
                SqlUpdateValueWrapper::Null => {
                    if no > 0 {
                        sql.push(',');
                    }
                    no += 1;

                    sql.push_str("NULL");
                }
                SqlUpdateValueWrapper::Value { metadata, value } => {
                    if no > 0 {
                        sql.push(',');
                    }
                    no += 1;

                    let pos = sql.len();
                    value.write(sql, params, &metadata);

                    if let Some(params_with_index) = params_with_index {
                        let param = &sql[pos..];

                        if param.starts_with('$') {
                            params_with_index.insert(field_name, params.len());
                        }
                    }
                }
            }
        }

        sql.push(')');
    }

    fn build_insert_sql(
        &'s self,
        table_name: &str,
        params: &mut Vec<crate::SqlValue<'s>>,
        mut params_with_index: Option<HashMap<&'static str, usize>>,
    ) -> (String, Option<HashMap<&'static str, usize>>) {
        if Self::get_e_tag_column_name().is_some() {
            let value = rust_extensions::date_time::DateTimeAsMicroseconds::now();
            self.set_e_tag_value(value.unix_microseconds);
        }

        let mut result = String::new();

        result.push_str("INSERT INTO ");
        result.push_str(table_name);

        Self::generate_insert_fields(self, &mut result, params, &mut params_with_index);

        (result, params_with_index)
    }

    fn build_bulk_insert_sql(
        table_name: &str,
        models: &'s [impl SqlInsertModel<'s>],
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let mut result = String::new();

        result.push_str("INSERT INTO ");
        result.push_str(table_name);
        result.push_str(" (");

        let fields_amount = Self::get_fields_amount();

        for no in 0..fields_amount {
            if no > 0 {
                result.push(',');
            }
            result.push_str(Self::get_field_name(no));
        }

        result.push_str(") VALUES ");
        let mut model_no = 0;
        let mut params = Vec::new();
        for model in models {
            if Self::get_e_tag_column_name().is_some() {
                let value = rust_extensions::date_time::DateTimeAsMicroseconds::now();
                model.set_e_tag_value(value.unix_microseconds);
            }

            if model_no > 0 {
                result.push(',');
            }
            model_no += 1;
            result.push('(');

            let mut written_no = 0;

            for no in 0..fields_amount {
                match model.get_field_value(no) {
                    SqlUpdateValueWrapper::Ignore => {}
                    SqlUpdateValueWrapper::Value { value, metadata } => {
                        if written_no > 0 {
                            result.push(',');
                        }

                        written_no += 1;
                        value.write(&mut result, &mut params, &metadata);
                    }
                    SqlUpdateValueWrapper::Null => {
                        if written_no > 0 {
                            result.push(',');
                        }

                        written_no += 1;
                        result.push_str("NULL");
                    }
                }
            }

            result.push(')');
        }

        (result, params)
    }

    fn build_insert_or_update_sql<TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
        table_name: &str,
        update_conflict_type: &crate::UpdateConflictType<'s>,
        model: &'s TSqlInsertModel,
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let mut params = Vec::new();

        let update_fields = HashMap::new();
        let (mut sql, update_fields) =
            model.build_insert_sql(table_name, &mut params, Some(update_fields));

        update_conflict_type.generate_sql(&mut sql);

        sql.push_str(" DO UPDATE SET ");

        model.build_update_sql_part(&mut sql, &mut params, update_fields.as_ref());

        (sql, params)
    }

    fn build_bulk_insert_or_update_sql<TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
        table_name: &str,
        update_conflict_type: &crate::UpdateConflictType<'s>,
        insert_or_update_models: &'s [TSqlInsertModel],
    ) -> Vec<(String, Vec<crate::SqlValue<'s>>)> {
        let mut sql = Vec::new();

        for model in insert_or_update_models {
            set_e_tag(model);
            sql.push(TSqlInsertModel::build_insert_or_update_sql(
                table_name,
                update_conflict_type,
                model,
            ));
        }

        sql
    }
}

fn set_e_tag<'s, TSqlInsertModel: SqlInsertModel<'s>>(model: &TSqlInsertModel) {
    if TSqlInsertModel::get_e_tag_column_name().is_some() {
        let value = rust_extensions::date_time::DateTimeAsMicroseconds::now();
        model.set_e_tag_value(value.unix_microseconds);
    }
}
