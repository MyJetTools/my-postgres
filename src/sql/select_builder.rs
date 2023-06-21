pub enum SelectFieldValue {
    LineNo(usize),
    Field(&'static str),
    Json(&'static str),
    DateTimeAsBigint(&'static str),
    DateTimeAsTimestamp(&'static str),
}

pub struct SelectBuilder {
    items: Vec<SelectFieldValue>,
}

impl SelectBuilder {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn push(&mut self, value: SelectFieldValue) {
        self.items.push(value)
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
}
