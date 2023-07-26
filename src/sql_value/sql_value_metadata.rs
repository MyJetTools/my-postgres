pub struct SqlValueMetadata {
    pub sql_type: Option<&'static str>,
    pub related_column_name: Option<&'static str>,
    pub wrap_column_name: bool,
}
