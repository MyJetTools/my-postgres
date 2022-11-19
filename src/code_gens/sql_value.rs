use rust_extensions::{date_time::DateTimeAsMicroseconds, StrOrString};

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
    pub fn as_sql_value_to_injext(&self) -> StrOrString {
        match self {
            SqlValue::ByIndex(value) => StrOrString::crate_as_string(format!("${}", value)),
            SqlValue::Str(value) => StrOrString::crate_as_str(value),
            SqlValue::String(value) => StrOrString::crate_as_str(value),
            SqlValue::Bool(value) => {
                if *value {
                    StrOrString::crate_as_str(TRUE)
                } else {
                    StrOrString::crate_as_str(FALSE)
                }
            }

            SqlValue::I8(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::U8(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::I16(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::U16(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::I32(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::U32(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::I64(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::U64(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::F32(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::F64(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::ISize(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::USize(value) => StrOrString::crate_as_string(format!("{}", value)),
            SqlValue::DateTime(value) => {
                StrOrString::crate_as_string(format!("'{}'", value.to_rfc3339()))
            }
            SqlValue::DateTimeAsUnixMicroseconds(value) => {
                StrOrString::crate_as_string(format!("'{}'", value.unix_microseconds))
            }
        }
    }
}
