use crate::{sql_select::SelectEntity, sql_where::SqlWhereModel};

pub enum SelectFieldValue {
    LineNo(usize),
    Field(&'static str),
    Json(&'static str),
    DateTimeAsBigint(&'static str),
    DateTimeAsTimestamp(&'static str),
}

impl SelectFieldValue {
    pub fn unwrap_as_line_no(&self) -> usize {
        match self {
            SelectFieldValue::LineNo(line_no) => *line_no,
            SelectFieldValue::Field(field_name) => panic!("Value is Field: {}", field_name),
            SelectFieldValue::Json(field_name) => panic!("Value is Json: {}", field_name),
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                panic!("Value is DateTimeAsBigint: {}", field_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                panic!("Value is DateTimeAsTimestamp: {}", field_name)
            }
        }
    }

    pub fn unwrap_as_field(&self) -> &'static str {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field(field_name) => field_name,
            SelectFieldValue::Json(field_name) => panic!("Value is Json: {}", field_name),
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                panic!("Value is DateTimeAsBigint: {}", field_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                panic!("Value is DateTimeAsTimestamp: {}", field_name)
            }
        }
    }

    pub fn unwrap_as_json(&self) -> &'static str {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field(field_name) => panic!("Value is Field: {}", field_name),
            SelectFieldValue::Json(field_name) => field_name,
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                panic!("Value is DateTimeAsBigint: {}", field_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                panic!("Value is DateTimeAsTimestamp: {}", field_name)
            }
        }
    }

    pub fn unwrap_as_date_time_as_bigint(&self) -> &'static str {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field(field_name) => panic!("Value is Field: {}", field_name),
            SelectFieldValue::Json(field_name) => panic!("Value is Json: {}", field_name),
            SelectFieldValue::DateTimeAsBigint(field_name) => field_name,
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                panic!("Value is DateTimeAsTimestamp: {}", field_name)
            }
        }
    }

    pub fn unwrap_as_date_time_as_timestamp(&self) -> &'static str {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field(field_name) => panic!("Value is Field: {}", field_name),
            SelectFieldValue::Json(field_name) => panic!("Value is Json: {}", field_name),
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                panic!("Value is DateTimeAsBigint: {}", field_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => field_name,
        }
    }
}

pub struct SelectBuilder {
    items: Vec<SelectFieldValue>,
    order_by_columns: Option<&'static str>,
    group_by_columns: Option<&'static str>,
}

impl SelectBuilder {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            order_by_columns: None,
            group_by_columns: None,
        }
    }

    pub fn from_select_model<TSelectEntity: SelectEntity>() -> Self {
        let mut builder = Self::new();
        TSelectEntity::fill_select_fields(&mut builder);

        builder.group_by_columns = TSelectEntity::get_group_by_fields();
        builder.order_by_columns = TSelectEntity::get_order_by_fields();

        builder
    }

    pub fn push(&mut self, value: SelectFieldValue) {
        self.items.push(value)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn get(&self, index: usize) -> Option<&SelectFieldValue> {
        self.items.get(index)
    }

    pub fn fill_select_fields(&self, sql: &mut String) {
        let mut no = 0;
        for value in &self.items {
            if no > 0 {
                sql.push_str(",");
            }

            match value {
                SelectFieldValue::Field(field_name) => {
                    sql.push_str(field_name);
                }
                SelectFieldValue::Json(field_name) => {
                    sql.push_str(field_name);
                    sql.push_str(" #>> '{}' as \"");
                    sql.push_str(field_name);
                    sql.push('"');
                }
                SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                    sql.push_str("(extract(EPOCH FROM ");
                    sql.push_str(field_name);
                    sql.push_str(") * 1000000)::bigint as \"");
                    sql.push_str(field_name);
                    sql.push('"');
                }
                SelectFieldValue::DateTimeAsBigint(field_name) => {
                    sql.push_str(field_name);
                }
                SelectFieldValue::LineNo(line_no) => {
                    sql.push_str(format!("{}::int as \"line_no\"", line_no).as_str());
                }
            }

            no += 1;
        }
    }

    pub fn build_select_sql<'s>(
        &self,
        table_name: &str,
        where_model: Option<&'s impl SqlWhereModel<'s>>,
    ) -> (String, Vec<crate::SqlValue<'s>>) {
        let mut sql = String::new();
        let mut params = Vec::new();

        sql.push_str("SELECT ");

        sql.push_str(" FROM ");
        sql.push_str(table_name);

        if let Some(where_model) = where_model {
            let where_condition = where_model.build_where_sql_part(&mut params);

            if where_condition.has_conditions() {
                sql.push_str(" WHERE ");
                where_condition.build(&mut sql);
            }
        }

        if let Some(order_by_fields) = self.order_by_columns {
            sql.push_str(order_by_fields);
        }

        if let Some(group_by_fields) = self.group_by_columns {
            sql.push_str(group_by_fields);
        }

        if let Some(where_model) = where_model {
            where_model.fill_limit_and_offset(&mut sql);
        }

        (sql, params)
    }
}
