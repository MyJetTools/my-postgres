use std::collections::HashMap;

use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::Serialize;

use crate::{sql::SqlValues, SqlValueMetadata};

use super::SqlUpdateBuilderValue;

pub trait SqlUpdateValueProvider<'s> {
    fn get_value_to_update(
        &'s self,
        params: &mut SqlValues<'s>,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s>;
}

impl<'s> SqlUpdateValueProvider<'s> for String {
    fn get_value_to_update(
        &'s self,
        params: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        let index = params.push(self.as_str());
        SqlUpdateBuilderValue::Index(index)
    }
}

impl<'s> SqlUpdateValueProvider<'s> for &'s str {
    fn get_value_to_update(
        &'s self,
        params: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        let index = params.push(*self);
        SqlUpdateBuilderValue::Index(index)
    }
}

impl<'s> SqlUpdateValueProvider<'s> for DateTimeAsMicroseconds {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        if let Some(metadata) = &metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "bigint" {
                    return SqlUpdateBuilderValue::NonStringValue(
                        self.unix_microseconds.to_string().into(),
                    );
                }

                if sql_type == "timestamp" {
                    return SqlUpdateBuilderValue::StringValue(self.to_rfc3339().into());
                }

                panic!("Unknown sql type: {}", sql_type);
            }
        }

        panic!("DateTimeAsMicroseconds requires sql_type");
    }
}

impl<'s> SqlUpdateValueProvider<'s> for bool {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        match self {
            true => SqlUpdateBuilderValue::NonStringValue("true".into()),
            false => SqlUpdateBuilderValue::NonStringValue("false".into()),
        }
    }
}

impl<'s> SqlUpdateValueProvider<'s> for u8 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for i8 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for u16 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for f32 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for f64 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for i16 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for u32 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for i32 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for u64 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s> SqlUpdateValueProvider<'s> for i64 {
    fn get_value_to_update(
        &'s self,
        _: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        SqlUpdateBuilderValue::NonStringValue(self.to_string().into())
    }
}

impl<'s, T: Serialize> SqlUpdateValueProvider<'s> for Vec<T> {
    fn get_value_to_update(
        &'s self,
        params: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        let as_string = serde_json::to_string(self).unwrap();
        let index = params.push(as_string);
        SqlUpdateBuilderValue::Json(index)

        /*
        sql.push_str("cast($");
        sql.push_str(params.len().to_string().as_str());
        sql.push_str("::text as json)");
         */
    }
}

impl<'s, TKey: Serialize, TVale: Serialize> SqlUpdateValueProvider<'s> for HashMap<TKey, TVale> {
    fn get_value_to_update(
        &'s self,
        params: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateBuilderValue<'s> {
        let as_string = serde_json::to_string(self).unwrap();
        let index = params.push(as_string);

        SqlUpdateBuilderValue::Json(index)

        /*

        */
    }
}
