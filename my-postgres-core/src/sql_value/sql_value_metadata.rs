use crate::table_schema::TableColumnType;

#[derive(Debug)]
pub struct SqlValueMetadata {
    pub sql_type: TableColumnType,
    pub operator: Option<&'static str>,
    pub wrap_column_name: Option<&'static str>,
}
