use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    SqlValueMetadata,
};

pub struct GroupBySum(i32);

impl GroupBySum {
    pub fn get_value(&self) -> i32 {
        self.0
    }
}

impl SelectValueProvider for GroupBySum {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(crate::sql::SelectFieldValue::GroupByField {
            field_name,
            statement: format!("SUM({})", field_name).into(),
        });
    }
}

impl FromDbRow<GroupBySum> for GroupBySum {
    fn from_db_row(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupBySum {
        GroupBySum(row.get(name))
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupBySum> {
        let result: Option<i32> = row.get(name);
        Some(GroupBySum(result?))
    }
}
