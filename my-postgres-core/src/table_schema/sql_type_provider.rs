use std::collections::{BTreeMap, HashMap};

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::SqlValueMetadata;

use super::TableColumnType;

pub trait SqlTypeProvider {
    fn get_sql_type(metadata: Option<SqlValueMetadata>) -> TableColumnType;
}

impl SqlTypeProvider for u8 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl SqlTypeProvider for Option<u8> {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl SqlTypeProvider for i8 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl SqlTypeProvider for Option<i8> {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl SqlTypeProvider for u16 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Integer
    }
}

impl SqlTypeProvider for i16 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl SqlTypeProvider for u32 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Integer
    }
}

impl SqlTypeProvider for i32 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Integer
    }
}

impl SqlTypeProvider for u64 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl SqlTypeProvider for i64 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl SqlTypeProvider for usize {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl SqlTypeProvider for isize {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl SqlTypeProvider for f32 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Real
    }
}

impl SqlTypeProvider for f64 {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Double
    }
}

impl SqlTypeProvider for String {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Text
    }
}

impl SqlTypeProvider for Option<String> {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Text
    }
}

impl<T> SqlTypeProvider for Vec<T> {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Json
    }
}

impl<TKey, TValue> SqlTypeProvider for HashMap<TKey, TValue> {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Jsonb
    }
}

impl<TKey, TValue> SqlTypeProvider for BTreeMap<TKey, TValue> {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Jsonb
    }
}

impl SqlTypeProvider for bool {
    fn get_sql_type(_metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Boolean
    }
}

impl SqlTypeProvider for DateTimeAsMicroseconds {
    fn get_sql_type(metadata: Option<SqlValueMetadata>) -> TableColumnType {
        if let Some(metadata) = metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "timestamp" {
                    return TableColumnType::Timestamp;
                }

                if sql_type == "bigint" {
                    return TableColumnType::BigInt;
                }
            }
        }

        panic!("Sql type is not set")
    }
}
