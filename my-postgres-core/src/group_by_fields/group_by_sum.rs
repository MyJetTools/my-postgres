use tokio_postgres::types::FromSql;

use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    DbColumnName, GroupByFieldType, SqlValueMetadata,
};
#[derive(Debug)]
pub struct GroupBySum<T: Send + Sync + 'static>(T);

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> GroupBySum<T> {
    pub fn get_value(&self) -> T {
        self.0
    }
}

impl<'s, T: GroupByFieldType + Send + Sync + 'static> SelectValueProvider for GroupBySum<T> {
    fn fill_select_part(
        sql: &mut SelectBuilder,
        column_name: DbColumnName,
        metadata: &Option<SqlValueMetadata>,
    ) {
        let sql_type = if let Some(metadata) = metadata {
            metadata.sql_type
        } else {
            T::DB_SQL_TYPE
        };

        sql.push(crate::sql::SelectFieldValue::GroupByField {
            statement: format!(
                "SUM({})::{}",
                column_name.db_column_name,
                sql_type.as_db_type_str()
            )
            .into(),
            column_name,
        });
    }
}

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> FromDbRow<'s, GroupBySum<T>>
    for GroupBySum<T>
{
    fn from_db_row(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupBySum<T> {
        GroupBySum(row.get(column_name.field_name))
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupBySum<T>> {
        let result: Option<T> = row.get(column_name.field_name);
        Some(GroupBySum(result?))
    }
}
