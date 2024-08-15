use std::collections::{BTreeMap, HashMap};

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    sql::{SelectBuilder, SelectFieldValue},
    DbColumnName, SqlValueMetadata,
};

pub trait SelectValueProvider {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    );
}

impl SelectValueProvider for String {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl<'s> SelectValueProvider for &'s str {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for usize {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for i64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for u64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for i32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for u32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for i16 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for u16 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for i8 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for u8 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for f64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for f32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl SelectValueProvider for bool {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(column_name));
    }
}

impl<T> SelectValueProvider for Vec<T> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Json(column_name));
    }
}

impl<TKey, TValue> SelectValueProvider for HashMap<TKey, TValue> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Json(column_name));
    }
}

impl<TKey, TValue> SelectValueProvider for BTreeMap<TKey, TValue> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Json(column_name));
    }
}

impl SelectValueProvider for DateTimeAsMicroseconds {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if let Some(metadata) = metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "timestamp" {
                    sql.push(SelectFieldValue::DateTimeAsTimestamp(column_name));
                    return;
                }

                if sql_type == "bigint" {
                    sql.push(SelectFieldValue::DateTimeAsBigint(column_name));

                    return;
                }

                panic!("Field: {:?}. Unknown sql_type: {}", column_name, sql_type);
            }
        }

        panic!(
            "Field: {:?}.  sql_type is required for DateTimeAsMicroseconds",
            column_name
        );
    }
}
