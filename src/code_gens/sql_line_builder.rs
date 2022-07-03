use super::SqlValue;

pub struct SqlLineBuilder {
    result: String,
    separator: char,
}

impl SqlLineBuilder {
    pub fn new(separator: char) -> Self {
        Self {
            result: "".to_string(),
            separator,
        }
    }

    pub fn has_value(&self) -> bool {
        self.result.len() > 0
    }

    pub fn add(&mut self, value: &str) {
        if self.result.len() > 0 {
            self.result.push(self.separator);
        }

        self.result.push_str(value);
    }

    pub fn add_sql_value(&mut self, sql_value: &SqlValue) {
        if self.result.len() > 0 {
            self.result.push(self.separator);
        }

        self.result
            .push_str(sql_value.as_sql_value_to_injext().as_str());
    }

    pub fn add_update(&mut self, field_name: &str, sql_value: &SqlValue) {
        if self.result.len() > 0 {
            self.result.push(self.separator);
        }

        self.result.push_str(field_name);
        self.result.push('=');
        self.result
            .push_str(sql_value.as_sql_value_to_injext().as_str());
    }

    pub fn as_str(&self) -> &str {
        self.result.as_str()
    }
}
