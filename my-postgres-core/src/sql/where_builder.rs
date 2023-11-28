use crate::ColumnName;

#[derive(Debug)]
pub struct JsonPropertyValueProvider {
    pub db_column_name: &'static str,
    pub json_property_name: &'static str,
    pub value: SqlWhereValue,
}

impl JsonPropertyValueProvider {
    pub fn push_json_field_name(&self, sql: &mut String) {
        sql.push('"');
        sql.push_str(self.db_column_name);
        sql.push_str("\"->>'");
        sql.push_str(self.json_property_name);
        sql.push('\'');
    }
}

#[derive(Debug)]
pub enum SqlWhereValue {
    None,
    Index(usize),
    NonStringValue(String),
    StringValue(String),
    VecOfValues(Box<Vec<SqlWhereValue>>),
    VecOfJsonProperties(Box<Vec<JsonPropertyValueProvider>>),
}

impl SqlWhereValue {
    pub fn print_field_name_at_generate_sql(&self) -> bool {
        match self {
            Self::VecOfJsonProperties(_) => false,
            _ => true,
        }
    }
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
            SqlWhereValue::VecOfJsonProperties(value) => {
                panic!("Type is VecOfJsonProperties: {:?}", value)
            }
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
            SqlWhereValue::VecOfJsonProperties(value) => {
                panic!("Type is VecOfJsonProperties: {:?}", value)
            }
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
                fill_vec_of_values(sql, &values);
                true
            }

            SqlWhereValue::VecOfJsonProperties(properties) => {
                fill_vec_of_json_properties(sql, &properties);

                true
            }
        }
    }
}

fn fill_vec_of_values(sql: &mut String, values: &Vec<SqlWhereValue>) {
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
                in_no += 1;
            }
            SqlWhereValue::VecOfValues(_) => {}
            SqlWhereValue::VecOfJsonProperties(json_properties) => {
                fill_vec_of_json_properties(sql, &json_properties);
                in_no += 1;
            }
        }
    }

    sql.push(')');
}

fn fill_vec_of_json_properties(sql: &mut String, properties: &Vec<JsonPropertyValueProvider>) {
    sql.push('(');
    let mut in_no = 0;
    for json_property in properties.as_slice() {
        if in_no > 0 {
            sql.push_str(" AND ");
        }
        match &json_property.value {
            SqlWhereValue::None => {}
            SqlWhereValue::Index(val_index) => {
                json_property.push_json_field_name(sql);
                sql.push_str("=$");
                sql.push_str(val_index.to_string().as_str());
                in_no += 1;
            }
            SqlWhereValue::NonStringValue(value) => {
                json_property.push_json_field_name(sql);
                sql.push_str("=");
                sql.push_str(value.as_str());
                in_no += 1;
            }
            SqlWhereValue::StringValue(value) => {
                json_property.push_json_field_name(sql);
                sql.push_str("=");
                sql.push('\'');
                sql.push_str(value.as_str());
                sql.push('\'');
            }
            SqlWhereValue::VecOfValues(values) => {
                fill_vec_of_values(sql, &values);
            }
            SqlWhereValue::VecOfJsonProperties(_) => {}
        }
    }

    sql.push(')');
}

pub enum WhereBuilder {
    Fields(WhereBuilderFromFields),
    Raw(String),
}

impl WhereBuilder {
    pub fn build(&self, sql: &mut String) {
        match self {
            WhereBuilder::Fields(fields) => fields.build(sql),
            WhereBuilder::Raw(the_sql) => sql.push_str(the_sql),
        }
    }

    pub fn has_conditions(&self) -> bool {
        match self {
            WhereBuilder::Fields(fields) => fields.has_conditions(),
            WhereBuilder::Raw(_) => true,
        }
    }

    pub fn unwrap_as_fields(self) -> WhereBuilderFromFields {
        match self {
            WhereBuilder::Fields(fields) => fields,
            WhereBuilder::Raw(_) => panic!("WhereBuilder is Raw"),
        }
    }
}

pub struct WhereCondition {
    pub db_column_name: ColumnName,
    pub op: &'static str,
    pub value: SqlWhereValue,
}

pub struct WhereBuilderFromFields {
    conditions: Vec<WhereCondition>,
}

impl WhereBuilderFromFields {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    pub fn push_where_condition(
        &mut self,
        db_column_name: ColumnName,
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

            if condition.value.print_field_name_at_generate_sql() {
                condition.db_column_name.push_name(sql);
                sql.push_str(condition.op);
            }
            condition.value.push_value(sql);
            index += 1;
        }
    }
}
