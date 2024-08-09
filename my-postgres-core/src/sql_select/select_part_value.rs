use std::collections::{BTreeMap, HashMap};

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    sql::{SelectBuilder, SelectFieldValue},
    SqlValueMetadata,
};

pub trait SelectValueProvider {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        metadata: &Option<SqlValueMetadata>,
    );
}

impl SelectValueProvider for String {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl<'s> SelectValueProvider for &'s str {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for usize {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for i64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for u64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for i32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for u32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for i16 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for u16 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for i8 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for u8 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for f64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for f32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl SelectValueProvider for bool {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Field(db_column_name));
    }
}

impl<T> SelectValueProvider for Vec<T> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Json(db_column_name));
    }
}

impl<TKey, TValue> SelectValueProvider for HashMap<TKey, TValue> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Json(db_column_name));
    }
}

impl<TKey, TValue> SelectValueProvider for BTreeMap<TKey, TValue> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::Json(db_column_name));
    }
}

impl SelectValueProvider for DateTimeAsMicroseconds {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if let Some(metadata) = metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "timestamp" {
                    sql.push(SelectFieldValue::DateTimeAsTimestamp(db_column_name));
                    return;
                }

                if sql_type == "bigint" {
                    sql.push(SelectFieldValue::DateTimeAsBigint(db_column_name));

                    return;
                }

                panic!("Field: {}. Unknown sql_type: {}", db_column_name, sql_type);
            }
        }

        panic!(
            "Field: {}.  sql_type is required for DateTimeAsMicroseconds",
            db_column_name
        );
    }
}
