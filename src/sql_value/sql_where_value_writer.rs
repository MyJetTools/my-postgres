use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{sql::SqlWhereValue, SqlValueMetadata};

pub trait SqlWhereValueProvider {
    fn get_where_value(
        &self,
        params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue;

    fn get_default_operator(&self) -> &'static str;

    fn is_none(&self) -> bool;
}

impl SqlWhereValueProvider for String {
    fn get_where_value(
        &self,
        params: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        let index = params.push(self.to_string());
        SqlWhereValue::Index(index)
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider for &'s str {
    fn get_where_value(
        &self,
        params: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        let index = params.push(self.to_string());
        SqlWhereValue::Index(index)
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for DateTimeAsMicroseconds {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        if let Some(metadata) = &metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "bigint" {
                    return SqlWhereValue::NonStringValue(
                        self.unix_microseconds.to_string().into(),
                    );
                }

                if sql_type == "timestamp" {
                    return SqlWhereValue::StringValue(self.to_rfc3339().into());
                }

                panic!("Unknown sql type: {}", sql_type);
            }
        }

        panic!("DateTimeAsMicroseconds requires sql_type");
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for bool {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        match self {
            true => SqlWhereValue::NonStringValue("true".into()),
            false => SqlWhereValue::NonStringValue("false".into()),
        }
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for u8 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for i8 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }
    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for u16 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for f32 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for f64 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }
    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for i16 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }
    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for u32 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for i32 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for u64 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for i64 {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlWhereValueProvider for tokio_postgres::types::IsNull {
    fn get_where_value(
        &self,
        _: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        match self {
            tokio_postgres::types::IsNull::Yes => {
                return SqlWhereValue::NonStringValue("NULL".into());
            }
            tokio_postgres::types::IsNull::No => {
                return SqlWhereValue::NonStringValue("NOT NULL".into());
            }
        }
    }

    fn get_default_operator(&self) -> &'static str {
        " IS "
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<T: SqlWhereValueProvider> SqlWhereValueProvider for Vec<T> {
    fn get_where_value(
        &self,
        params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        if self.len() == 1 {
            return self.get(0).unwrap().get_where_value(params, metadata);
        }

        if self.len() > 0 {
            let mut result = Vec::with_capacity(self.len());
            for itm in self {
                let item = itm.get_where_value(params, metadata);
                result.push(item);
            }

            return SqlWhereValue::VecOfValues(Box::new(result));
        }

        SqlWhereValue::None
    }

    fn get_default_operator(&self) -> &'static str {
        if self.len() == 0 {
            return "";
        } else if self.len() == 1 {
            return "=";
        } else {
            return " IN ";
        }
    }

    fn is_none(&self) -> bool {
        false
    }
}
