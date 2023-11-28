use std::collections::{BTreeMap, HashMap};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::Serialize;

use crate::{
    sql::{SqlUpdateValue, SqlValues},
    SqlValueMetadata,
};

pub trait SqlUpdateValueProvider {
    fn get_update_value(
        &self,
        params: &mut SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue;
}

impl SqlUpdateValueProvider for String {
    fn get_update_value(
        &self,
        params: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        let index = params.push(self.into());
        SqlUpdateValue::Index(index)
    }
}

impl<'s> SqlUpdateValueProvider for &'s str {
    fn get_update_value(
        &self,
        params: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        let index = params.push((*self).into());
        SqlUpdateValue::Index(index)
    }
}

impl SqlUpdateValueProvider for DateTimeAsMicroseconds {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        if let Some(metadata) = &metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "bigint" {
                    return SqlUpdateValue::NonStringValue(
                        self.unix_microseconds.to_string().into(),
                    );
                }

                if sql_type == "timestamp" {
                    return SqlUpdateValue::StringValue(self.to_rfc3339().into());
                }

                panic!("Unknown sql type: {}", sql_type);
            }
        }

        panic!("DateTimeAsMicroseconds requires sql_type");
    }
}

impl SqlUpdateValueProvider for bool {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        match self {
            true => SqlUpdateValue::NonStringValue("true".into()),
            false => SqlUpdateValue::NonStringValue("false".into()),
        }
    }
}

impl SqlUpdateValueProvider for u8 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for i8 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for u16 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for f32 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for f64 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for i16 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for u32 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for i32 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for u64 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl SqlUpdateValueProvider for i64 {
    fn get_update_value(
        &self,
        _: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.to_string().into())
    }
}

impl<T: Serialize> SqlUpdateValueProvider for Vec<T> {
    fn get_update_value(
        &self,
        params: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        let as_string = serde_json::to_string(self).unwrap();
        let index = params.push(as_string.into());
        SqlUpdateValue::Json(index)

        /*
        sql.push_str("cast($");
        sql.push_str(params.len().to_string().as_str());
        sql.push_str("::text as json)");
         */
    }
}

impl<TKey: Serialize, TVale: Serialize> SqlUpdateValueProvider for HashMap<TKey, TVale> {
    fn get_update_value(
        &self,
        params: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        let as_string = serde_json::to_string(self).unwrap();
        let index = params.push(as_string.into());

        SqlUpdateValue::Json(index)
    }
}

impl<TKey: Serialize, TVale: Serialize> SqlUpdateValueProvider for BTreeMap<TKey, TVale> {
    fn get_update_value(
        &self,
        params: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        let as_string = serde_json::to_string(self).unwrap();
        let index = params.push(as_string.into());

        SqlUpdateValue::Json(index)
    }
}
