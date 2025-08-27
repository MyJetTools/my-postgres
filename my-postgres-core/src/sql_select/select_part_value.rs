use std::collections::{BTreeMap, HashMap};

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    sql::{SelectBuilder, SelectFieldValue},
    table_schema::TableColumnType,
    DbColumnName, SqlValueMetadata,
};

const DB_TYPE_TEXT: &str = "text";

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
        metadata: &Option<SqlValueMetadata>,
    ) {
        if column_name.force_cast_db_type {
            sql.push(SelectFieldValue::FieldWithCast {
                column_name,
                cast_to: DB_TYPE_TEXT,
            });

            return;
        }

        if let Some(meta_data) = metadata {
            if !meta_data.sql_type.is_string() {
                sql.push(SelectFieldValue::FieldWithCast {
                    column_name,
                    cast_to: "text",
                });
                return;
            }
        }

        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl<'s> SelectValueProvider for &'s str {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if column_name.force_cast_db_type {
            sql.push(SelectFieldValue::FieldWithCast {
                column_name,
                cast_to: DB_TYPE_TEXT,
            });
        } else {
            sql.push(SelectFieldValue::create_as_field(column_name, metadata));
        }
    }
}

impl SelectValueProvider for usize {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for i64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for u64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for i32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for u32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for i16 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for u16 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for i8 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for u8 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for f64 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for f32 {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
    }
}

impl SelectValueProvider for bool {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(SelectFieldValue::create_as_field(column_name, metadata));
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
            match metadata.sql_type {
                TableColumnType::Timestamp => {
                    sql.push(SelectFieldValue::DateTimeAsTimestamp(column_name));
                    return;
                }
                TableColumnType::BigInt => {
                    sql.push(SelectFieldValue::DateTimeAsBigint(column_name));
                    return;
                }
                _ => {
                    panic!(
                        "Field: {:?}. Unsupported sql_type: {:?} for DateTimeAsMicroseconds",
                        column_name, metadata.sql_type
                    );
                }
            }
        }

        panic!(
            "Field: {:?}.  sql_type is required for DateTimeAsMicroseconds",
            column_name
        );
    }
}
