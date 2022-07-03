use super::SqlValue;

pub struct WhereBuilder {
    operator: &'static str,
    values: Vec<(String, SqlValue)>,
}

impl WhereBuilder {
    pub fn new(operator: &'static str) -> Self {
        Self {
            operator,
            values: Vec::new(),
        }
    }

    pub fn add(&mut self, field_name: &str, value: SqlValue) {
        self.values.push((field_name.to_string(), value));
    }

    pub fn build(&self, dest: &mut String) {
        let mut number = 0;
        for (field_name, value) in &self.values {
            if number > 0 {
                dest.push(' ');
                dest.push_str(&self.operator);
                dest.push(' ');
            }

            dest.push_str(field_name.as_str());
            dest.push('=');
            dest.push_str(value.as_sql_value_to_injext().as_str());
            number += 1;
        }
    }
}
