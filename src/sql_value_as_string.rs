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
