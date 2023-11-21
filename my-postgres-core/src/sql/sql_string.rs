const STR_SIZE: usize = 32;

#[derive(Debug)]
pub enum NonStringValue {
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
}

#[derive(Debug)]
pub enum SqlString {
    AsString(String),
    AsStr(&'static str),
    Str32([u8; STR_SIZE]),
    NonStrValue(NonStringValue),
}

impl SqlString {
    pub fn from_str(src: &str) -> Self {
        let src_as_bytes = src.as_bytes();
        if src_as_bytes.len() < STR_SIZE {
            let mut value = [0u8; STR_SIZE];
            value[0] = src_as_bytes.len() as u8;
            value[1..1 + src.len()].copy_from_slice(src_as_bytes);
            return Self::Str32(value);
        }

        Self::AsString(src.to_string())
    }

    pub fn from_static_str(src: &'static str) -> Self {
        Self::AsStr(src)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            SqlString::AsString(value) => Some(value.as_str()),
            SqlString::AsStr(value) => Some(*value),
            SqlString::Str32(value) => Some(get_pascal_str(value)),
            _ => None,
        }
    }
}

impl Into<SqlString> for String {
    fn into(self) -> SqlString {
        SqlString::AsString(self)
    }
}

impl<'s> Into<SqlString> for &'s str {
    fn into(self) -> SqlString {
        SqlString::from_str(self)
    }
}

impl<'s> Into<SqlString> for &'s String {
    fn into(self) -> SqlString {
        SqlString::from_str(self)
    }
}

fn get_pascal_str(src: &[u8]) -> &str {
    let len = src[0] as usize;
    std::str::from_utf8(&src[1..len + 1]).unwrap()
}

impl tokio_postgres::types::ToSql for SqlString {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            SqlString::AsString(value) => value.as_str().to_sql(ty, out),
            SqlString::AsStr(value) => value.to_sql(ty, out),
            SqlString::Str32(value) => get_pascal_str(value).to_sql(ty, out),
            SqlString::NonStrValue(value) => match value {
                NonStringValue::SmallInt(value) => (*value).to_sql(ty, out),
                NonStringValue::Integer(value) => (*value).to_sql(ty, out),
                NonStringValue::BigInt(value) => (*value).to_sql(ty, out),
                NonStringValue::Float(value) => (*value).to_sql(ty, out),
                NonStringValue::Double(value) => (*value).to_sql(ty, out),
            },
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool
    where
        Self: Sized,
    {
        String::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            SqlString::AsString(value) => value.as_str().to_sql_checked(ty, out),
            SqlString::AsStr(value) => value.to_sql_checked(ty, out),
            SqlString::Str32(value) => get_pascal_str(value).to_sql_checked(ty, out),
            SqlString::NonStrValue(value) => match value {
                NonStringValue::SmallInt(value) => (*value).to_sql_checked(ty, out),
                NonStringValue::Integer(value) => (*value).to_sql_checked(ty, out),
                NonStringValue::BigInt(value) => (*value).to_sql_checked(ty, out),
                NonStringValue::Float(value) => (*value).to_sql_checked(ty, out),
                NonStringValue::Double(value) => (*value).to_sql_checked(ty, out),
            },
        }
    }
}
