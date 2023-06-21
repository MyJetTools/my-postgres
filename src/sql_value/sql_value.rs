#[derive(Debug)]
pub enum SqlValue<'s> {
    ValueAsString(String),
    ValueAsStaticStr(&'static str),
    Ref(&'s (dyn tokio_postgres::types::ToSql + Sync)),
}

impl<'s> SqlValue<'s> {
    pub fn get_value(&'s self) -> &'s (dyn tokio_postgres::types::ToSql + Sync) {
        match self {
            SqlValue::ValueAsString(value) => value,
            SqlValue::Ref(value) => *value,
            SqlValue::ValueAsStaticStr(value) => value,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            SqlValue::ValueAsString(value) => value.clone(),
            SqlValue::Ref(value) => {
                let result = format!("{:?}", value);

                if result.starts_with('"') {
                    return result[1..result.len() - 1].to_string();
                }

                result
            }
            SqlValue::ValueAsStaticStr(value) => value.to_string(),
        }
    }
}
