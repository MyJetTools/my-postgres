#[derive(Debug)]
pub struct SqlValueMetadata {
    pub sql_type: Option<&'static str>,
    pub operator: Option<&'static str>,
    pub wrap_column_name: Option<&'static str>,
}
