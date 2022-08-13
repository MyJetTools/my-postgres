use super::{
    super::{NumberedParams, SqlLineBuilder, SqlValue},
    InsertCodeGen,
};

pub struct InsertBuilder<'s> {
    fields: SqlLineBuilder,
    values: SqlLineBuilder,
    numbered_params: NumberedParams<'s>,
}

impl<'s> InsertBuilder<'s> {
    pub fn new() -> Self {
        Self {
            fields: SqlLineBuilder::new(','),
            values: SqlLineBuilder::new(','),

            numbered_params: NumberedParams::new(),
        }
    }

    pub fn build(&self, table_name: &str) -> String {
        format!(
            "INSERT INTO {table_name} ({fields}) VALUES ({values})",
            fields = self.fields.as_str(),
            values = self.values.as_str(),
        )
    }

    pub fn get_values_data(&mut self) -> &'s [&(dyn tokio_postgres::types::ToSql + Sync)] {
        self.numbered_params.build_params()
    }
}

impl<'s> InsertCodeGen<'s> for InsertBuilder<'s> {
    fn append_field(&mut self, field_name: &str, value: SqlValue<'s>) {
        let value = self.numbered_params.add_or_get(value);
        self.fields.add(field_name);
        self.values.add_sql_value(&value);
    }
}
