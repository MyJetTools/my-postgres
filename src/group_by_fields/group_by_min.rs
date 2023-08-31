use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    SqlValueMetadata,
};

pub struct GroupByMin(i32);

impl GroupByMin {
    pub fn get_value(&self) -> i32 {
        self.0
    }
}

impl SelectValueProvider for GroupByMin {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if let Some(metadata) = metadata {
            if let Some(sql_type) = metadata.sql_type {
                sql.push(crate::sql::SelectFieldValue::GroupByField {
                    field_name,
                    statement: format!("MIN({})::{}", field_name, sql_type).into(),
                });
                return;
            }
        }

        sql.push(crate::sql::SelectFieldValue::GroupByField {
            field_name,
            statement: format!("MIN({})::int", field_name).into(),
        });
    }
}

impl FromDbRow<GroupByMin> for GroupByMin {
    fn from_db_row(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByMin {
        GroupByMin(row.get(name))
    }

    fn from_db_row_opt(
        row: &crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByMin> {
        let result: Option<i32> = row.get(name);
        Some(GroupByMin(result?))
    }
}
