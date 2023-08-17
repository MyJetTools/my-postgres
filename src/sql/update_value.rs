pub enum SqlUpdateValue {
    Index(usize),
    StringValue(String),
    NonStringValue(String),
    Json(usize),
}

impl SqlUpdateValue {
    pub fn unwrap_as_index(&self) -> usize {
        match self {
            SqlUpdateValue::Index(index) => *index,
            SqlUpdateValue::StringValue(value) => panic!("Type is StringValue: {}", value.as_str()),
            SqlUpdateValue::NonStringValue(value) => {
                panic!("Type is NonStringValue: {}", value.as_str())
            }
            SqlUpdateValue::Json(value) => {
                panic!("Type is Json: {}", value)
            }
        }
    }

    pub fn unwrap_as_string_value(&self) -> &str {
        match self {
            SqlUpdateValue::Index(index) => panic!("Type is Index: {}", index),
            SqlUpdateValue::StringValue(value) => value,
            SqlUpdateValue::NonStringValue(value) => {
                panic!("Type is NonStringValue: {}", value.as_str())
            }
            SqlUpdateValue::Json(value) => {
                panic!("Type is Json: {}", value)
            }
        }
    }

    pub fn unwrap_as_non_string_value(&self) -> &str {
        match self {
            SqlUpdateValue::Index(index) => {
                panic!("Type is Index: ({})", index)
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
            SqlUpdateValue::Index(index) => {
                panic!("Type is Index: ({})", index)
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
            SqlUpdateValue::Index(index) => {
                sql.push('$');
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
