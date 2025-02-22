use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::de::DeserializeOwned;

use crate::{DbColumnName, SqlValueMetadata};

pub trait FromDbRow<'s, TResult> {
    fn from_db_row(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) -> TResult;

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) -> Option<TResult>;
}

impl<'s> FromDbRow<'s, String> for String {
    fn from_db_row(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> String {
        row.get(column_name.db_column_name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<String> {
        row.get(column_name.db_column_name)
    }
}

impl<'s> FromDbRow<'s, i64> for i64 {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> i64 {
        row.get(column_name.db_column_name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<i64> {
        row.get(column_name.db_column_name)
    }
}

impl<'s> FromDbRow<'s, u64> for u64 {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> u64 {
        let result: i64 = row.get(column_name.db_column_name);
        result as u64
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<u64> {
        let result: Option<i64> = row.get(column_name.db_column_name);
        let result = result?;
        Some(result as u64)
    }
}

impl<'s> FromDbRow<'s, i32> for i32 {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> i32 {
        row.get(column_name.db_column_name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<i32> {
        row.get(column_name.db_column_name)
    }
}

impl<'s> FromDbRow<'s, u32> for u32 {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> u32 {
        let result: i64 = row.get(column_name.db_column_name);
        result as u32
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<u32> {
        let result: Option<i64> = row.get(column_name.db_column_name);
        let result = result?;
        Some(result as u32)
    }
}

impl<'s> FromDbRow<'s, bool> for bool {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        row.get(column_name.db_column_name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<bool> {
        row.get(column_name.db_column_name)
    }
}

impl<'s, T: DeserializeOwned> FromDbRow<'s, Vec<T>> for Vec<T> {
    fn from_db_row(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Vec<T> {
        let value: String = row.get(column_name.db_column_name);
        serde_json::from_str(&value).unwrap()
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<Vec<T>> {
        let value: Option<String> = row.get(column_name.db_column_name);

        let value = value.as_ref()?;
        let result = serde_json::from_str(value).unwrap();
        Some(result)
    }
}

impl<'s, TKey: DeserializeOwned + Eq + Hash, TValue: DeserializeOwned>
    FromDbRow<'s, HashMap<TKey, TValue>> for HashMap<TKey, TValue>
{
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> HashMap<TKey, TValue> {
        let value: String = row.get(column_name.db_column_name);
        serde_json::from_str(&value).unwrap()
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<HashMap<TKey, TValue>> {
        let value: Option<String> = row.get(column_name.db_column_name);
        let value = value.as_ref()?;
        let result = serde_json::from_str(value).unwrap();
        Some(result)
    }
}

impl<'s, TKey: DeserializeOwned + Eq + Hash + Ord, TValue: DeserializeOwned>
    FromDbRow<'s, BTreeMap<TKey, TValue>> for BTreeMap<TKey, TValue>
{
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> BTreeMap<TKey, TValue> {
        let value: String = row.get(column_name.db_column_name);
        serde_json::from_str(&value).unwrap()
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<BTreeMap<TKey, TValue>> {
        let value: Option<String> = row.get(column_name.db_column_name);
        let value = value.as_ref()?;
        let result = serde_json::from_str(value).unwrap();
        Some(result)
    }
}

impl<'s> FromDbRow<'s, f64> for f64 {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> f64 {
        row.get(column_name.db_column_name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<f64> {
        row.get(column_name.db_column_name)
    }
}

impl<'s> FromDbRow<'s, f32> for f32 {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> f32 {
        row.get(column_name.db_column_name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<f32> {
        row.get(column_name.db_column_name)
    }
}

impl<'s> FromDbRow<'s, DateTimeAsMicroseconds> for DateTimeAsMicroseconds {
    fn from_db_row(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> DateTimeAsMicroseconds {
        let mut db_column_name = String::new();
        crate::utils::fill_adjusted_column_name(column_name.db_column_name, &mut db_column_name);
        let unix_microseconds: i64 = row.get(db_column_name.as_str());
        DateTimeAsMicroseconds::new(unix_microseconds)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<DateTimeAsMicroseconds> {
        let mut db_column_name = String::new();
        crate::utils::fill_adjusted_column_name(column_name.db_column_name, &mut db_column_name);
        let unix_microseconds: Option<i64> = row.get(db_column_name.as_str());
        let unix_microseconds = unix_microseconds?;
        Some(DateTimeAsMicroseconds::new(unix_microseconds))
    }
}
