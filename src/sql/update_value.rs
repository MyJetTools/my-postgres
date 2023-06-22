use rust_extensions::StrOrString;

pub enum SqlUpdateValue<'s> {
    Index(usize, Option<usize>),
    StringValue(StrOrString<'s>),
    NonStringValue(StrOrString<'s>),
    Json(usize),
}

impl<'s> SqlUpdateValue<'s> {
    pub fn unwrap_as_index(&self) -> (usize, Option<usize>) {
        match self {
            SqlUpdateValue::Index(index, index2) => (*index, *index2),
            SqlUpdateValue::StringValue(value) => panic!("Type is StringValue: {}", value.as_str()),
            SqlUpdateValue::NonStringValue(value) => {
                panic!("Type is NonStringValue: {}", value.as_str())
            }
            SqlUpdateValue::Json(value) => {
                panic!("Type is Json: {}", value)
            }
        }
    }

    pub fn unwrap_as_string_value(&'s self) -> &StrOrString<'s> {
        match self {
            SqlUpdateValue::Index(index, index2) => panic!("Type is Index: {}/{:?}", index, index2),
            SqlUpdateValue::StringValue(value) => value,
            SqlUpdateValue::NonStringValue(value) => {
                panic!("Type is NonStringValue: {}", value.as_str())
            }
            SqlUpdateValue::Json(value) => {
                panic!("Type is Json: {}", value)
            }
        }
    }

    pub fn unwrap_as_non_string_value(&'s self) -> &StrOrString<'s> {
        match self {
            SqlUpdateValue::Index(index, index2) => {
                panic!("Type is Index: ({},{:?})", index, index2)
            }
            SqlUpdateValue::StringValue(value) => panic!("Type is StringValue: {}", value.as_str()),
            SqlUpdateValue::NonStringValue(value) => value,
            SqlUpdateValue::Json(value) => {
                panic!("Type is Json: {}", value)
            }
        }
    }

    pub fn unwrap_as_json(&self) -> usize {
        match self {
            SqlUpdateValue::Index(index, index2) => {
                panic!("Type is Index: ({},{:?})", index, index2)
            }
            SqlUpdateValue::StringValue(value) => panic!("Type is StringValue: {}", value.as_str()),
            SqlUpdateValue::NonStringValue(value) => {
                panic!("Type is NonStringValue: {}", value.as_str())
            }
            SqlUpdateValue::Json(value) => *value,
        }
    }

    pub fn write(&self, sql: &mut String) {
        match self {
            SqlUpdateValue::Index(index, index2) => {
                sql.push('$');
                sql.push_str(index.to_string().as_str());

                if let Some(index2) = index2 {
                    sql.push_str(",$");
                    sql.push_str(index2.to_string().as_str());
                }
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
