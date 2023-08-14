use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    SqlValueMetadata,
};

pub struct GroupByAvg(i32);

impl GroupByAvg {
    pub fn get_value(&self) -> i32 {
        self.0
    }
}

impl SelectValueProvider for GroupByAvg {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push(crate::sql::SelectFieldValue::GroupByField {
            field_name,
            statement: format!("AVG({})", field_name).into(),
        });
    }
}

impl FromDbRow<GroupByAvg> for GroupByAvg {
    fn from_db_row(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByAvg {
        GroupByAvg(row.get(name))
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByAvg> {
        let result: Option<i32> = row.get(name);
        Some(GroupByAvg(result?))
    }
}
