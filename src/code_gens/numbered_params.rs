use std::collections::{BTreeMap, HashMap};

use super::SqlValue;

pub struct NumberedParams<'s> {
    params: HashMap<String, u32>,
    param_no: u32,
    by_index: BTreeMap<u32, &'s str>,
    as_vec: Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
}

impl<'s> NumberedParams<'s> {
    pub fn new() -> Self {
        Self {
            params: HashMap::new(),
            param_no: 1,
            as_vec: Vec::new(),
            by_index: BTreeMap::new(),
        }
    }

    pub fn add_or_get(&mut self, sql_value: SqlValue<'s>) -> SqlValue<'s> {
        match &sql_value {
            SqlValue::Str(value) => {
                if !self.params.contains_key(*value) {
                    let result = self.param_no;
                    self.params.insert(value.to_string(), result);
                    self.param_no += 1;
                    return SqlValue::ByIndex(result);
                }

                return SqlValue::ByIndex(*self.params.get(*value).unwrap());
            }
            SqlValue::String(value) => {
                if !self.params.contains_key(value) {
                    let result = self.param_no;
                    self.params.insert(value.to_string(), result);
                    self.param_no += 1;
                    return SqlValue::ByIndex(result);
                }

                return SqlValue::ByIndex(*self.params.get(value).unwrap());
            }
            _ => {
                return sql_value.clone();
            }
        }
    }

    pub fn build_params(&'s mut self) -> &'s [&(dyn tokio_postgres::types::ToSql + Sync)] {
        for (key, value) in &self.params {
            self.by_index.insert(*value, key);
        }

        for value in self.by_index.values() {
            self.as_vec.push(value);
        }

        self.as_vec.as_slice()
    }
}
