use rust_extensions::StrOrString;

pub enum SqlUpdateBuilderValue<'s> {
    Index(usize),
    StringValue(StrOrString<'s>),
    NonStringValue(StrOrString<'s>),
    Json(usize),
}

impl<'s> SqlUpdateBuilderValue<'s> {
    pub fn write(&self, sql: &mut String) {
        match self {
            SqlUpdateBuilderValue::Index(index) => {
                sql.push_str("$");
                sql.push_str(index.to_string().as_str());
            }
            SqlUpdateBuilderValue::StringValue(value) => {
                sql.push_str("'");
                sql.push_str(value.as_str());
                sql.push_str("'");
            }
            SqlUpdateBuilderValue::NonStringValue(value) => {
                sql.push_str(value.as_str());
            }
            SqlUpdateBuilderValue::Json(index) => {
                sql.push_str("cast($");
                sql.push_str(index.to_string().as_str());
                sql.push_str("::text as json)");
            }
        }
    }
}
