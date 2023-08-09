use crate::ColumnName;

pub struct UpsertColumns {
    columns: Option<Vec<ColumnName>>,
}

impl UpsertColumns {
    pub fn new_as_active() -> Self {
        Self {
            columns: Some(Vec::new()),
        }
    }

    pub fn as_none() -> Self {
        Self { columns: None }
    }

    pub fn is_active(&self) -> bool {
        self.columns.is_some()
    }

    pub fn push(&mut self, column_name: ColumnName) {
        if let Some(columns) = self.columns.as_mut() {
            columns.push(column_name);
        }
    }

    pub fn as_slice(&self) -> &[ColumnName] {
        if let Some(columns) = self.columns.as_ref() {
            columns.as_slice()
        } else {
            &[]
        }
    }
}
