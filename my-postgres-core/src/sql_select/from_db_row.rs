use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::de::DeserializeOwned;

use crate::SqlValueMetadata;

pub trait FromDbRow<'s, TResult> {
    fn from_db_row(
        row: &'s crate::DbRow,
        name: &str,
        metadata: &Option<SqlValueMetadata>,
    ) -> TResult;

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        name: &str,
        metadata: &Option<SqlValueMetadata>,
    ) -> Option<TResult>;
}

impl<'s> FromDbRow<'s, String> for String {
    fn from_db_row(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> String {
        row.get(name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<String> {
        row.get(name)
    }
}

impl<'s> FromDbRow<'s, i64> for i64 {
    fn from_db_row(row: &crate::DbRow, name: &str, _metadata: &Option<SqlValueMetadata>) -> i64 {
        row.get(name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<i64> {
        row.get(name)
    }
}

impl<'s> FromDbRow<'s, u64> for u64 {
    fn from_db_row(row: &crate::DbRow, name: &str, _metadata: &Option<SqlValueMetadata>) -> u64 {
        let result: i64 = row.get(name);
        result as u64
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<u64> {
        let result: Option<i64> = row.get(name);
        let result = result?;
        Some(result as u64)
    }
}

impl<'s> FromDbRow<'s, i32> for i32 {
    fn from_db_row(row: &crate::DbRow, name: &str, _metadata: &Option<SqlValueMetadata>) -> i32 {
        row.get(name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<i32> {
        row.get(name)
    }
}

impl<'s> FromDbRow<'s, u32> for u32 {
    fn from_db_row(row: &crate::DbRow, name: &str, _metadata: &Option<SqlValueMetadata>) -> u32 {
        let result: i64 = row.get(name);
        result as u32
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<u32> {
        let result: Option<i64> = row.get(name);
        let result = result?;
        Some(result as u32)
    }
}

impl<'s> FromDbRow<'s, bool> for bool {
    fn from_db_row(row: &crate::DbRow, name: &str, _metadata: &Option<SqlValueMetadata>) -> bool {
        row.get(name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<bool> {
        row.get(name)
    }
}

impl<'s, T: DeserializeOwned> FromDbRow<'s, Vec<T>> for Vec<T> {
    fn from_db_row(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Vec<T> {
        let value: String = row.get(name);
        serde_json::from_str(&value).unwrap()
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<Vec<T>> {
        let value: Option<String> = row.get(name);

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
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> HashMap<TKey, TValue> {
        let value: String = row.get(name);
        serde_json::from_str(&value).unwrap()
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<HashMap<TKey, TValue>> {
        let value: Option<String> = row.get(name);
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
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> BTreeMap<TKey, TValue> {
        let value: String = row.get(name);
        serde_json::from_str(&value).unwrap()
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<BTreeMap<TKey, TValue>> {
        let value: Option<String> = row.get(name);
        let value = value.as_ref()?;
        let result = serde_json::from_str(value).unwrap();
        Some(result)
    }
}

impl<'s> FromDbRow<'s, f64> for f64 {
    fn from_db_row(row: &crate::DbRow, name: &str, _metadata: &Option<SqlValueMetadata>) -> f64 {
        row.get(name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<f64> {
        row.get(name)
    }
}

impl<'s> FromDbRow<'s, f32> for f32 {
    fn from_db_row(row: &crate::DbRow, name: &str, _metadata: &Option<SqlValueMetadata>) -> f32 {
        row.get(name)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<f32> {
        row.get(name)
    }
}

impl<'s> FromDbRow<'s, DateTimeAsMicroseconds> for DateTimeAsMicroseconds {
    fn from_db_row(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> DateTimeAsMicroseconds {
        let unix_microseconds: i64 = row.get(name);
        DateTimeAsMicroseconds::new(unix_microseconds)
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<DateTimeAsMicroseconds> {
        let unix_microseconds: Option<i64> = row.get(name);
        let unix_microseconds = unix_microseconds?;
        Some(DateTimeAsMicroseconds::new(unix_microseconds))
    }
}
