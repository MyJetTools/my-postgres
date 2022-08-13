use super::super::{sql_line_builder::SqlLineBuilder, NumberedParams, SqlValue};

pub struct InsertOrUpdateBuilder<'s> {
    insert_fields: SqlLineBuilder,
    insert_values: SqlLineBuilder,
    update_fields: SqlLineBuilder,

    numbered_params: NumberedParams<'s>,
}

impl<'s> InsertOrUpdateBuilder<'s> {
    pub fn new() -> Self {
        Self {
            insert_fields: SqlLineBuilder::new(','),
            insert_values: SqlLineBuilder::new(','),
            update_fields: SqlLineBuilder::new(','),

            numbered_params: NumberedParams::new(),
        }
    }

    pub fn add_field(&mut self, field_name: &str, sql_value: SqlValue<'s>, is_primary_key: bool) {
        let sql_value = self.numbered_params.add_or_get(sql_value);

        self.insert_fields.add(field_name);
        self.insert_values.add_sql_value(&sql_value);

        if !is_primary_key {
            self.update_fields.add_update(field_name, &sql_value);
        }
    }

    pub fn build(&self, table_name: &str, pk_name: &str) -> String {
        let mut result = String::new();

        result.push_str("INSERT INTO  ");
        result.push_str(table_name);
        result.push_str(" (");
        result.push_str(self.insert_fields.as_str());
        result.push_str(") VALUES (");
        result.push_str(self.insert_values.as_str());
        result.push_str(") ON CONFLICT ON CONSTRAINT ");
        result.push_str(pk_name);
        result.push_str(" DO UPDATE SET ");
        result.push_str(self.update_fields.as_str());

        result
    }

    pub fn get_values_data(&mut self) -> &'s [&(dyn tokio_postgres::types::ToSql + Sync)] {
        self.numbered_params.build_params()
    }
}
