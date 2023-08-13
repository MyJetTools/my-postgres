use crate::{sql::SelectBuilder, SqlValueMetadata};

use super::{FromDbRow, SelectValueProvider};

pub struct CountWithGroupValue(i32);

impl CountWithGroupValue {
    pub fn get_value(&self) -> i32 {
        self.0
    }
}

impl SelectValueProvider for CountWithGroupValue {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(crate::sql::SelectFieldValue::CountWithGroupBy(field_name));
    }
}

impl FromDbRow<CountWithGroupValue> for CountWithGroupValue {
    fn from_db_row(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> CountWithGroupValue {
        CountWithGroupValue(row.get(name))
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<CountWithGroupValue> {
        let result: Option<i32> = row.get(name);
        Some(CountWithGroupValue(result?))
    }
}
