use tokio_postgres::types::FromSql;

use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    GroupByFieldType, SqlValueMetadata,
};

pub struct GroupBySum<T: Send + Sync + 'static>(T);

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> GroupBySum<T> {
    pub fn get_value(&self) -> T {
        self.0
    }
}

impl<'s, T: GroupByFieldType + Send + Sync + 'static> SelectValueProvider for GroupBySum<T> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        metadata: &Option<SqlValueMetadata>,
    ) {
        let sql_type = if let Some(metadata) = metadata {
            if let Some(sql_type) = metadata.sql_type {
                sql_type
            } else {
                T::DB_SQL_TYPE
            }
        } else {
            T::DB_SQL_TYPE
        };

        sql.push(crate::sql::SelectFieldValue::GroupByField {
            field_name,
            statement: format!("SUM({field_name})::{}", sql_type).into(),
        });
    }
}

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> FromDbRow<'s, GroupBySum<T>>
    for GroupBySum<T>
{
    fn from_db_row(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupBySum<T> {
        GroupBySum(row.get(name))
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupBySum<T>> {
        let result: Option<T> = row.get(name);
        Some(GroupBySum(result?))
    }
}
