use rust_extensions::StrOrString;

pub enum SqlWhereValue<'s> {
    None,
    Index(usize),
    NonStringValue(StrOrString<'s>),
    StringValue(StrOrString<'s>),
    VecOfValues(Box<Vec<SqlWhereValue<'s>>>),
}

impl<'s> SqlWhereValue<'s> {
    pub fn unwrap_is_index(&self) -> usize {
        match self {
            SqlWhereValue::Index(index) => *index,
            _ => panic!("unwrap_is_index"),
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

pub struct WhereCondition<'s> {
    pub db_column_name: &'static str,
    pub op: &'static str,
    pub value: SqlWhereValue<'s>,
}

pub struct WhereBuilder<'s> {
    conditions: Vec<WhereCondition<'s>>,
}

impl<'s> WhereBuilder<'s> {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    pub fn push_where_condition(
        &mut self,
        db_column_name: &'static str,
        op: &'static str,
        value: SqlWhereValue<'s>,
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

    pub fn get(&self, index: usize) -> Option<&WhereCondition<'s>> {
        self.conditions.get(index)
    }

    pub fn build(&self, sql: &mut String) {
        let mut result = String::new();
        let mut index = 0;
        for condition in &self.conditions {
            if index > 0 {
                result.push_str(" AND ");
            }
            result.push_str(condition.db_column_name);
            result.push_str(condition.op);
            condition.value.push_value(sql);
            index += 1;
        }
    }
}
