use crate::{sql_update::SqlUpdateModel, SqlUpdateValueWrapper};

pub trait SqlInsertModel<'s> {
    fn get_fields_amount() -> usize;
    fn get_field_name(no: usize) -> (&'static str, Option<&'static str>);
    fn get_field_value(&'s self, no: usize) -> SqlUpdateValueWrapper<'s>;

    fn get_e_tag_column_name() -> Option<&'static str>;
    fn get_e_tag_value(&self) -> Option<i64>;
    fn set_e_tag_value(&self, value: i64);

    fn generate_insert_fields(sql: &mut String) {
        sql.push('(');
        let mut no = 0;
        for field_no in 0..Self::get_fields_amount() {
            if no > 0 {
                sql.push(',');
            }
            no += 1;
            let (field_name, additional_field_name) = Self::get_field_name(field_no);
            sql.push_str(field_name);
            if let Some(additional_field_name) = additional_field_name {
                sql.push(',');
                sql.push_str(additional_field_name);
                no += 1;
            }
        }

        sql.push(')');
    }

    fn generate_insert_fields_values(
        &'s self,
        sql: &mut String,
        params: &mut Vec<crate::SqlValue<'s>>,
    ) {
        sql.push('(');
        for field_no in 0..Self::get_fields_amount() {
            let update_value = self.get_field_value(field_no);

            match &update_value.value {
                Some(value) => {
                    if field_no > 0 {
                        sql.push(',');
                    }

                    value.write(sql, params, &update_value.metadata);
                }
                None => {
                    if field_no > 0 {
                        sql.push(',');
                    }

                    sql.push_str("NULL");
                }
            }
        }
        sql.push(')');
    }

    fn build_insert_sql(&'s self, table_name: &str) -> (String, Vec<crate::SqlValue<'s>>) {
        if Self::get_e_tag_column_name().is_some() {
            let value = rust_extensions::date_time::DateTimeAsMicroseconds::now();
            self.set_e_tag_value(value.unix_microseconds);
        }

        let mut sql = String::new();

        let mut params = Vec::new();

        sql.push_str("INSERT INTO ");
        sql.push_str(table_name);
        Self::generate_insert_fields(&mut sql);
        sql.push_str(" VALUES ");
        self.generate_insert_fields_values(&mut sql, &mut params);

        (sql, params)
    }

    fn fill_bulk_insert_values_sql(
        models: &'s [impl SqlInsertModel<'s>],
        sql: &mut String,
        params: &mut Vec<crate::SqlValue<'s>>,
    ) {
        let mut model_no = 0;
        for model in models {
            if Self::get_e_tag_column_name().is_some() {
                let value = rust_extensions::date_time::DateTimeAsMicroseconds::now();
                model.set_e_tag_value(value.unix_microseconds);
            }

            if model_no > 0 {
                sql.push(',');
            }
            model_no += 1;
            model.generate_insert_fields_values(sql, params);
        }
    }

    fn build_bulk_insert_sql(
        table_name: &str,
        models: &'s [impl SqlInsertModel<'s>],
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let mut result = String::new();

        result.push_str("INSERT INTO ");
        result.push_str(table_name);

        Self::generate_insert_fields(&mut result);

        result.push_str(" VALUES ");

        let mut params = Vec::new();

        Self::fill_bulk_insert_values_sql(models, &mut result, &mut params);

        (result, params)
    }

    fn build_insert_or_update_sql<TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
        table_name: &str,
        update_conflict_type: &crate::UpdateConflictType<'s>,
        model: &'s TSqlInsertModel,
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let (mut sql, params) = model.build_insert_sql(table_name);

        update_conflict_type.generate_sql(&mut sql);

        sql.push_str(" DO UPDATE SET ");

        TSqlInsertModel::fill_upsert_sql_part(&mut sql);

        (sql, params)
    }

    fn build_bulk_insert_or_update_sql<TSqlInsertModel: SqlInsertModel<'s> + SqlUpdateModel<'s>>(
        table_name: &str,
        update_conflict_type: &crate::UpdateConflictType<'s>,
        insert_or_update_models: &'s [TSqlInsertModel],
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let (mut sql, params) = Self::build_bulk_insert_sql(table_name, insert_or_update_models);

        update_conflict_type.generate_sql(&mut sql);

        sql.push_str(" DO UPDATE SET ");

        TSqlInsertModel::fill_upsert_sql_part(&mut sql);

        (sql, params)
    }
}

fn set_e_tag<'s, TSqlInsertModel: SqlInsertModel<'s>>(model: &TSqlInsertModel) {
    if TSqlInsertModel::get_e_tag_column_name().is_some() {
        let value = rust_extensions::date_time::DateTimeAsMicroseconds::now();
        model.set_e_tag_value(value.unix_microseconds);
    }
}
