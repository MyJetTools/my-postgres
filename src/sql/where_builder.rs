#[derive(Debug)]
pub enum SqlWhereValue {
    None,
    Index(usize),
    NonStringValue(String),
    StringValue(String),
    VecOfValues(Box<Vec<SqlWhereValue>>),
}

impl SqlWhereValue {
    pub fn unwrap_as_index(&self) -> usize {
        match self {
            SqlWhereValue::Index(index) => *index,
            _ => panic!("unwrap_is_index"),
        }
    }

    pub fn unwrap_as_non_string_value(&self) -> &str {
        match self {
            SqlWhereValue::NonStringValue(value) => value,
            SqlWhereValue::None => panic!("Type is None"),
            SqlWhereValue::Index(value) => panic!("Type is Index: {:?}", value),
            SqlWhereValue::StringValue(value) => panic!("Type is StringValue: {}", value.as_str()),
            SqlWhereValue::VecOfValues(value) => panic!("Type is VecOfValues: {:?}", value),
        }
    }

    pub fn unwrap_as_string_value(&self) -> &str {
        match self {
            SqlWhereValue::StringValue(value) => value,
            SqlWhereValue::NonStringValue(value) => {
                panic!("Type is NonStringValue: {}", value.as_str())
            }
            SqlWhereValue::None => panic!("Type is None"),
            SqlWhereValue::Index(value) => panic!("Type is Index: {:?}", value),
            SqlWhereValue::VecOfValues(value) => panic!("Type is VecOfValues: {:?}", value),
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            SqlWhereValue::None => true,
            _ => false,
        }
    }

    pub fn push_value(&self, sql: &mut String) -> bool {
        match &self {
            SqlWhereValue::Index(index_value) => {
                sql.push('$');
                sql.push_str(index_value.to_string().as_str());
                true
            }
            SqlWhereValue::NonStringValue(value) => {
                sql.push_str(value.as_str());
                true
            }
            SqlWhereValue::StringValue(value) => {
                sql.push('\'');
                sql.push_str(value.as_str());
                sql.push('\'');
                true
            }
            SqlWhereValue::None => false,
            SqlWhereValue::VecOfValues(values) => {
                sql.push('(');

                let mut in_no = 0;
                for value in values.as_slice() {
                    if in_no > 0 {
                        sql.push(',');
                    }
                    match value {
                        SqlWhereValue::None => {}
                        SqlWhereValue::Index(val_index) => {
                            sql.push('$');
                            sql.push_str(val_index.to_string().as_str());
                            in_no += 1;
                        }
                        SqlWhereValue::NonStringValue(value) => {
                            sql.push_str(value.as_str());
                            in_no += 1;
                        }
                        SqlWhereValue::StringValue(value) => {
                            sql.push('\'');
                            sql.push_str(value.as_str());
                            sql.push('\'');
                        }
                        SqlWhereValue::VecOfValues(_) => {}
                    }
                }

                sql.push(')');
                true
            }
        }
    }
}

pub struct WhereCondition {
    pub db_column_name: &'static str,
    pub op: &'static str,
    pub value: SqlWhereValue,
}

pub struct WhereBuilder {
    conditions: Vec<WhereCondition>,
}

impl WhereBuilder {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    pub fn push_where_condition(
        &mut self,
        db_column_name: &'static str,
        op: &'static str,
        value: SqlWhereValue,
    ) {
        self.conditions.push(WhereCondition {
            db_column_name,
            op,
            value,
        });
    }

    pub fn has_conditions(&self) -> bool {
        self.conditions.len() > 0
    }

    pub fn get(&self, index: usize) -> Option<&WhereCondition> {
        self.conditions.get(index)
    }

    pub fn build(&self, sql: &mut String) {
        let mut index = 0;
        for condition in &self.conditions {
            if index > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str(condition.db_column_name);
            sql.push_str(condition.op);
            condition.value.push_value(sql);
            index += 1;
        }
    }
}
