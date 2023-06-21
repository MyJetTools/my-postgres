use rust_extensions::StrOrString;

pub struct SelectFieldValue {
    pub value: StrOrString<'static>,
    pub alias: Option<StrOrString<'static>>,
}

pub struct SelectBuilder {
    items: Vec<SelectFieldValue>,
}

impl SelectBuilder {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn push(&mut self, value: StrOrString<'static>, alias: Option<StrOrString<'static>>) {
        self.items.push(SelectFieldValue { value, alias })
    }

    pub fn push_json(&mut self, field_name: &'static str) {
        self.items.push(SelectFieldValue {
            value: format!("{}>> '{}'", field_name, "{}").into(),
            alias: Some(field_name.into()),
        })
    }

    pub fn build_select_part(&self, sql: &mut String) {
        let mut no = 0;
        for item in &self.items {
            if no > 0 {
                sql.push_str(",");
            }

            sql.push_str(item.value.as_str());

            if let Some(alias) = item.alias.as_ref() {
                sql.push_str(" as ");
                sql.push_str(alias.as_str());
            }

            no += 1;
        }
    }
}
