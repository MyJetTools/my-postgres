#[derive(Debug)]
pub struct SqlValueMetadata {
    pub sql_type: Option<&'static str>,
    pub operator: Option<&'static str>,
    pub wrap_column_name: Option<&'static str>,
}

impl SqlValueMetadata {
    pub fn is_json_or_jsonb(&self) -> bool {
        if let Some(sql_type) = self.sql_type {
            return sql_type.eq_ignore_ascii_case("json") || sql_type.eq_ignore_ascii_case("jsonb");
        }

        false
    }
}
