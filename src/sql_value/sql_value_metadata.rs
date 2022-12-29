pub struct SqlValueMetadata {
    pub sql_type: Option<&'static str>,
    pub related_field_name: Option<&'static str>,
}

impl SqlValueMetadata {
    pub fn with_sql_type(sql_type: &'static str) -> Self {
        Self {
            sql_type: Some(sql_type),
            related_field_name: None,
        }
    }
}
