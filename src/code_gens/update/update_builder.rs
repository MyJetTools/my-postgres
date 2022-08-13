use super::super::{NumberedParams, SqlLineBuilder, SqlValue, WhereBuilder};

pub struct UpdateBuilder<'s> {
    update_fields: SqlLineBuilder,
    where_clause: WhereBuilder<'s>,
    numbered_params: NumberedParams<'s>,
}

impl<'s> UpdateBuilder<'s> {
    pub fn new() -> Self {
        Self {
            update_fields: SqlLineBuilder::new(','),
            where_clause: WhereBuilder::new("AND"),
            numbered_params: NumberedParams::new(),
        }
    }

    pub fn append_field(
        &mut self,
        field_name: &str,
        sql_value: SqlValue<'s>,
        is_primary_key: bool,
    ) {
        let sql_value = self.numbered_params.add_or_get(sql_value);

        if is_primary_key {
            self.where_clause.add(field_name, sql_value)
        } else {
            self.update_fields.add_update(field_name, &sql_value);
        }
    }

    pub fn build(&self, table_name: &str) -> String {
        let mut result = String::new();
        result.push_str("UPDATE ");
        result.push_str(table_name);
        result.push_str(" SET ");
        result.push_str(self.update_fields.as_str());
        result.push_str(" WHERE ");

        self.where_clause.build(&mut result);

        result
    }
    pub fn get_values_data(&mut self) -> &'s [&(dyn tokio_postgres::types::ToSql + Sync)] {
        self.numbered_params.build_params()
    }
}
