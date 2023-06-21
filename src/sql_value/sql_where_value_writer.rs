use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{sql::SqlWhereValue, SqlValue, SqlValueMetadata};

pub trait SqlWhereValueProvider<'s> {
    fn get_where_value(
        &'s self,
        params: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s>;

    fn get_default_operator(&self) -> &'static str;

    fn is_none(&self) -> bool;
}

impl<'s> SqlWhereValueProvider<'s> for String {
    fn get_where_value(
        &'s self,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        params.push(SqlValue::Ref(self));
        SqlWhereValue::Index(params.len())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for &'s str {
    fn get_where_value(
        &'s self,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        params.push(SqlValue::Ref(self));
        SqlWhereValue::Index(params.len())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for DateTimeAsMicroseconds {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
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

impl<'s> SqlWhereValueProvider<'s> for bool {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
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

impl<'s> SqlWhereValueProvider<'s> for u8 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for i8 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }
    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for u16 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for f32 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for f64 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }
    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for i16 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }
    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for u32 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for i32 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for u64 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for i64 {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlWhereValueProvider<'s> for tokio_postgres::types::IsNull {
    fn get_where_value(
        &'s self,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
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

impl<'s, T: SqlWhereValueProvider<'s>> SqlWhereValueProvider<'s> for Vec<T> {
    fn get_where_value(
        &'s self,
        params: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
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
