use rust_extensions::date_time::DateTimeAsMicroseconds;
const TRUE: &'static str = "true";
const FALSE: &'static str = "false";

pub enum SqlValueAsString<'s> {
    String(String),
    Str(&'s str),
}

impl<'s> SqlValueAsString<'s> {
    pub fn as_str(&self) -> &str {
        match self {
            SqlValueAsString::String(value) => value.as_str(),
            SqlValueAsString::Str(value) => value,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SqlValue {
    ByIndex(u32),
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
    ISize(isize),
    USize(usize),
    DateTime(DateTimeAsMicroseconds),
}

impl SqlValue {
    pub fn as_sql_value_to_injext(&self) -> SqlValueAsString {
        match self {
            SqlValue::ByIndex(value) => SqlValueAsString::String(format!("${}", value)),
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
            SqlValue::ISize(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::USize(value) => SqlValueAsString::String(format!("{}", value)),
            SqlValue::DateTime(value) => {
                SqlValueAsString::String(format!("'{}'", value.to_rfc3339()))
            }
        }
    }
}
