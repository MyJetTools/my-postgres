use tokio_postgres::types::FromSql;

use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    DbColumnName, GroupByFieldType, SqlValueMetadata,
};
#[derive(Debug)]
pub struct GroupByMax<T: Send + Sync + 'static>(T);

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> GroupByMax<T> {
    pub fn get_value(&self) -> T {
        self.0
    }
}

impl<'s, T: GroupByFieldType + Send + Sync + 'static> SelectValueProvider for GroupByMax<T> {
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
            column_name,
            statement: format!(
                "MAX({})::{}",
                column_name.db_column_name,
                sql_type.as_db_type_str()
            )
            .into(),
        });
    }
}

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> FromDbRow<'s, GroupByMax<T>>
    for GroupByMax<T>
{
    fn from_db_row(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByMax<T> {
        GroupByMax(row.get(column_name.field_name))
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        column_name: DbColumnName,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByMax<T>> {
        let result: Option<T> = row.get(column_name.field_name);
        Some(GroupByMax(result?))
    }
}
