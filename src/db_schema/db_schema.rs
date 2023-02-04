pub enum TableColumnType {
    Text,
    SmallInt,
    BigInt,
    Boolean,
    Real,
    Double,
    Integer,
    Json,
    Timestamp,
}

pub struct TableColumn {
    pub name: &'static str,
    pub sql_type: TableColumnType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
}

pub trait TableSchema {
    fn get_columns() -> Vec<TableColumn>;
}
