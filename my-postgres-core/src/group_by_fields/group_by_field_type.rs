use crate::table_schema::TableColumnType;

pub trait GroupByFieldType {
    const DB_SQL_TYPE: TableColumnType;
}

impl GroupByFieldType for i64 {
    const DB_SQL_TYPE: TableColumnType = TableColumnType::BigInt;
}

impl GroupByFieldType for i32 {
    const DB_SQL_TYPE: TableColumnType = TableColumnType::Integer;
}

impl GroupByFieldType for i16 {
    const DB_SQL_TYPE: TableColumnType = TableColumnType::SmallInt;
}

impl GroupByFieldType for f32 {
    const DB_SQL_TYPE: TableColumnType = TableColumnType::Real;
}

impl GroupByFieldType for f64 {
    const DB_SQL_TYPE: TableColumnType = TableColumnType::DoublePrecision;
}
