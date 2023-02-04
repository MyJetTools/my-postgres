pub enum SqlType {
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

pub struct DtoColumn {
    pub name: &'static str,
    pub sql_type: SqlType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
}

pub trait DtoSchema {
    fn get_columns() -> Vec<DtoColumn>;
}
