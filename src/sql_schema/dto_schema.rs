pub enum SqlType {
    Text,
    SmallInt,
    BigInt,
    Boolean,
    Real,
    Double,
    Integer,
    Json,
    Timestamp { sql_type: String },
}

pub struct DtoColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
}

pub trait DtoSchema {
    fn get_columns() -> Vec<DtoColumn>;
}
