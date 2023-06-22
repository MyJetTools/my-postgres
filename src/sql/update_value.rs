use rust_extensions::StrOrString;

pub enum SqlUpdateValue<'s> {
    Index(usize),
    StringValue(StrOrString<'s>),
    NonStringValue(StrOrString<'s>),
    Json(usize),
}

impl<'s> SqlUpdateValue<'s> {
    pub fn write(&self, sql: &mut String) {
        match self {
            SqlUpdateValue::Index(index) => {
                sql.push_str("$");
                sql.push_str(index.to_string().as_str());
            }
            SqlUpdateValue::StringValue(value) => {
                sql.push_str("'");
                sql.push_str(value.as_str());
                sql.push_str("'");
            }
            SqlUpdateValue::NonStringValue(value) => {
                sql.push_str(value.as_str());
            }
            SqlUpdateValue::Json(index) => {
                sql.push_str("cast($");
                sql.push_str(index.to_string().as_str());
                sql.push_str("::text as json)");
            }
        }
    }
}
