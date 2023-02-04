use std::collections::HashMap;

use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::Serialize;

use crate::{SqlValue, SqlValueMetadata};

pub trait SqlUpdateValueWriter<'s> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    );
}

impl<'s> SqlUpdateValueWriter<'s> for String {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        params.push(SqlValue::Ref(self));
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for &'s str {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        params.push(SqlValue::Ref(self));
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for DateTimeAsMicroseconds {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if let Some(metadata) = &metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "bigint" {
                    sql.push_str(self.unix_microseconds.to_string().as_str());
                    return;
                }

                if sql_type == "timestamp" {
                    sql.push('\'');
                    sql.push_str(self.to_rfc3339().as_str());
                    sql.push('\'');
                    return;
                }

                panic!("Unknown sql type: {}", sql_type);
            }
        }

        panic!("DateTimeAsMicroseconds requires sql_type");
    }
}

impl<'s> SqlUpdateValueWriter<'s> for bool {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        match self {
            true => sql.push_str("true"),
            false => sql.push_str("false"),
        }
    }
}

impl<'s> SqlUpdateValueWriter<'s> for u8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for i8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for u16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for f32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for f64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for i16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for u32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for i32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for u64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlUpdateValueWriter<'s> for i64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s, T: Serialize> SqlUpdateValueWriter<'s> for Vec<T> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        let as_string = serde_json::to_string(self).unwrap();
        params.push(SqlValue::ValueAsString(as_string));
        sql.push_str("cast($");
        sql.push_str(params.len().to_string().as_str());
        sql.push_str("::text as json)");
    }
}

impl<'s, TKey: Serialize, TVale: Serialize> SqlUpdateValueWriter<'s> for HashMap<TKey, TVale> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        let as_string = serde_json::to_string(self).unwrap();
        params.push(SqlValue::ValueAsString(as_string));
        sql.push_str("cast($");
        sql.push_str(params.len().to_string().as_str());
        sql.push_str("::text as json)");
    }
}
