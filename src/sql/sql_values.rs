#[derive(Debug)]
pub enum SqlString<'s> {
    Str(&'s str),
    String(String),
}

impl<'s> SqlString<'s> {
    pub fn as_str(&'s self) -> &'s str {
        match self {
            SqlString::Str(result) => {
                return result;
            }
            SqlString::String(result) => {
                return result.as_str();
            }
        }
    }
}

impl<'s> tokio_postgres::types::ToSql for SqlString<'s> {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            SqlString::Str(result) => {
                return result.to_sql(ty, out);
            }
            SqlString::String(result) => {
                return result.to_sql(ty, out);
            }
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
            SqlString::Str(result) => {
                return result.to_sql_checked(ty, out);
            }
            SqlString::String(result) => {
                return result.to_sql_checked(ty, out);
            }
        }
    }
}

impl<'s> Into<SqlString<'s>> for String {
    fn into(self) -> SqlString<'s> {
        SqlString::String(self)
    }
}

impl<'s> Into<SqlString<'s>> for &'s String {
    fn into(self) -> SqlString<'s> {
        SqlString::Str(self)
    }
}

impl<'s> Into<SqlString<'s>> for &'s str {
    fn into(self) -> SqlString<'s> {
        SqlString::Str(self)
    }
}

pub enum SqlValues<'s> {
    Values(Vec<SqlString<'s>>),
    Empty,
}

const EMPTY: SqlValues = SqlValues::Empty;
const EMPTY_VALUES: Vec<&'static (dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

impl<'s> SqlValues<'s> {
    pub fn new() -> Self {
        Self::Values(Vec::new())
    }

    fn get_index_from_cache(&self, value: &str) -> Option<usize> {
        match self {
            SqlValues::Values(values) => {
                for (idx, itm) in values.iter().enumerate() {
                    if itm.as_str() == value {
                        return Some(idx);
                    }
                }
            }
            _ => {}
        }

        None
    }

    pub fn push(&mut self, value: impl Into<SqlString<'s>>) -> usize {
        let value: SqlString<'s> = value.into();
        if let Some(result) = self.get_index_from_cache(value.as_str()) {
            return result;
        }

        match self {
            SqlValues::Values(values) => {
                values.push(value.into());

                let result = values.len();

                result
            }
            SqlValues::Empty => {
                panic!("SqlValues is read only")
            }
        }
    }

    pub fn get_values_to_invoke(&'s self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
        match self {
            SqlValues::Values(values) => {
                let mut result: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

                for value in values {
                    result.push(value);
                }

                return result;
            }
            SqlValues::Empty => EMPTY_VALUES.clone(),
        }
    }

    pub fn empty() -> &'static SqlValues<'s> {
        &EMPTY
    }

    pub fn len(&self) -> usize {
        match self {
            SqlValues::Values(values) => {
                return values.len();
            }
            SqlValues::Empty => {
                return 0;
            }
        }
    }
}
