use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::SqlValueMetadata;

use super::TableColumnType;

pub trait TableColumnTypeProvider {
    fn get_column_type(&self, metadata: Option<SqlValueMetadata>) -> TableColumnType;
}

impl TableColumnTypeProvider for u8 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl TableColumnTypeProvider for i8 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl TableColumnTypeProvider for u16 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Integer
    }
}

impl TableColumnTypeProvider for i16 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::SmallInt
    }
}

impl TableColumnTypeProvider for u32 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Integer
    }
}

impl TableColumnTypeProvider for i32 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Integer
    }
}

impl TableColumnTypeProvider for u64 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl TableColumnTypeProvider for i64 {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl TableColumnTypeProvider for usize {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl TableColumnTypeProvider for isize {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::BigInt
    }
}

impl TableColumnTypeProvider for String {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Text
    }
}

impl TableColumnTypeProvider for bool {
    fn get_column_type(&self, _metadata: Option<SqlValueMetadata>) -> TableColumnType {
        TableColumnType::Boolean
    }
}

impl TableColumnTypeProvider for DateTimeAsMicroseconds {
    fn get_column_type(&self, metadata: Option<SqlValueMetadata>) -> TableColumnType {
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
