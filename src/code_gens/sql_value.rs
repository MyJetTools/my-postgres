use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::SqlValueAsString;
const TRUE: &'static str = "true";
const FALSE: &'static str = "false";

#[derive(Debug, Clone)]
pub enum SqlValue<'s> {
    ByIndex(u32),
    Str(&'s str),
    String(String),
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    F32(f32),
    F64(f64),
    ISize(isize),
    USize(usize),
    DateTime(DateTimeAsMicroseconds),
    DateTimeAsUnixMicroseconds(DateTimeAsMicroseconds),
}

impl<'s> SqlValue<'s> {
    pub fn as_sql_value_to_injext(&self) -> SqlValueAsString {
        match self {
            SqlValue::ByIndex(value) => SqlValueAsString::String(format!("${}", value)),
            SqlValue::Str(value) => SqlValueAsString::Str(value),
            SqlValue::String(value) => SqlValueAsString::Str(value),
            SqlValue::Bool(value) => {
                if *value {
                    SqlValueAsString::Str(TRUE)
                } else {
                    SqlValueAsString::Str(FALSE)
                }
            }

            SqlValue::I8(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::U8(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::I16(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::U16(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::I32(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::U32(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::I64(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::U64(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::F32(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::F64(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::ISize(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::USize(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::DateTime(value) => {
                SqlValueAsString::String(format!("'{}'", value.to_rfc3339()))
            }
            SqlValue::DateTimeAsUnixMicroseconds(value) => {
                SqlValueAsString::String(format!("'{}'", value.unix_microseconds))
            }
        }
    }
}
