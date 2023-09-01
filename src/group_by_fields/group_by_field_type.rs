pub trait GroupByFieldType {
    const DB_SQL_TYPE: &'static str;
}

impl GroupByFieldType for i64 {
    const DB_SQL_TYPE: &'static str = "bigint";
}

impl GroupByFieldType for i32 {
    const DB_SQL_TYPE: &'static str = "int";
}

impl GroupByFieldType for i16 {
    const DB_SQL_TYPE: &'static str = "smallint";
}

impl GroupByFieldType for f32 {
    const DB_SQL_TYPE: &'static str = "real";
}

impl GroupByFieldType for f64 {
    const DB_SQL_TYPE: &'static str = "double precision";
}
