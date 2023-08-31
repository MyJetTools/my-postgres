use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    SqlValueMetadata,
};

pub struct GroupByCount(i32);

impl GroupByCount {
    pub fn get_value(&self) -> i32 {
        self.0
    }
}

impl SelectValueProvider for GroupByCount {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if let Some(metadata) = metadata {
            if let Some(sql_type) = metadata.sql_type {
                sql.push(crate::sql::SelectFieldValue::GroupByField {
                    field_name,
                    statement: format!("COUNT({})::{}", field_name, sql_type).into(),
                });
            }
        }

        sql.push(crate::sql::SelectFieldValue::GroupByField {
            field_name,
            statement: "COUNT(*)::int".into(),
        });
    }
}

impl FromDbRow<GroupByCount> for GroupByCount {
    fn from_db_row(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByCount {
        GroupByCount(row.get(name))
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByCount> {
        let result: Option<i32> = row.get(name);
        Some(GroupByCount(result?))
    }
}
