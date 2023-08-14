use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    SqlValueMetadata,
};

pub struct GroupByMax(i32);

impl GroupByMax {
    pub fn get_value(&self) -> i32 {
        self.0
    }
}

impl SelectValueProvider for GroupByMax {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(crate::sql::SelectFieldValue::GroupByField {
            field_name,
            statement: format!("MAX({})", field_name).into(),
        });
    }
}

impl FromDbRow<GroupByMax> for GroupByMax {
    fn from_db_row(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByMax {
        GroupByMax(row.get(name))
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByMax> {
        let result: Option<i32> = row.get(name);
        Some(GroupByMax(result?))
    }
}
