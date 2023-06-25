use super::sql_string::SqlString;

const EMPTY: SqlValues = SqlValues::Empty;
const EMPTY_VALUES: Vec<&'static (dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
pub enum SqlValues {
    Values(Vec<SqlString>),
    Empty,
}

impl SqlValues {
    pub fn new() -> Self {
        Self::Values(Vec::new())
    }

    fn get_index_from_cache(&self, value: &str) -> Option<usize> {
        match self {
            SqlValues::Values(values) => {
                for (idx, itm) in values.iter().enumerate() {
                    if itm.as_str() == value {
                        return Some(idx + 1);
                    }
                }
            }
            _ => {}
        }

        None
    }

    pub fn push(&mut self, value: SqlString) -> usize {
        let value: SqlString = value.into();
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

    pub fn push_static_str(&mut self, value: &'static str) -> usize {
        self.push(SqlString::from_static_str(value))
    }

    pub fn get_values_to_invoke(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
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

    pub fn empty() -> &'static SqlValues {
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

    pub fn get(&self, index: usize) -> Option<&SqlString> {
        match self {
            SqlValues::Values(values) => {
                return values.get(index);
            }
            SqlValues::Empty => {
                return None;
            }
        }
    }
}
