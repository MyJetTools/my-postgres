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

    pub fn write_related_field_name(
        src: &Option<SqlValueMetadata>,
        field_name: &'static str,
    ) -> Self {
        if let Some(src) = src {
            return Self {
                sql_type: src.sql_type,
                related_field_name: Some(field_name),
            };
        }

        Self {
            sql_type: None,
            related_field_name: Some(field_name),
        }
    }
}
