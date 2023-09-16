const STR_SIZE: usize = 32;
#[derive(Debug)]
pub enum SqlString {
    AsString(String),
    AsStr(&'static str),
    Str32([u8; STR_SIZE]),
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

impl SqlString {
    pub fn as_str(&self) -> &str {
        match self {
            SqlString::AsString(value) => value,
            SqlString::AsStr(value) => value,
            SqlString::Str32(value) => get_pascal_str(value),
        }
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
        self.as_str().to_sql(ty, out)
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
        self.as_str().to_sql_checked(ty, out)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_str_32() {
        let src = "TestStr";

        let sql_string = SqlString::from_str(src);

        assert_eq!(sql_string.as_str(), src);
    }
}
