pub enum DbSchemaSqlType {
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

pub struct DbColumn {
    pub name: &'static str,
    pub sql_type: DbSchemaSqlType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
}

pub trait DbSchema {
    fn get_columns() -> Vec<DbColumn>;
}
