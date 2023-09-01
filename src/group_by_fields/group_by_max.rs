use tokio_postgres::types::FromSql;

use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    GroupByFieldType, SqlValueMetadata,
};

pub struct GroupByMax<T: Send + Sync + 'static>(T);

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> GroupByMax<T> {
    pub fn get_value(&self) -> T {
        self.0
    }
}

impl<'s, T: GroupByFieldType + Send + Sync + 'static> SelectValueProvider for GroupByMax<T> {
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
            statement: format!("MAX({field_name})::{} as {field_name}", sql_type).into(),
        });
    }
}

impl<'s, T: Copy + FromSql<'s> + Send + Sync + 'static> FromDbRow<'s, GroupByMax<T>>
    for GroupByMax<T>
{
    fn from_db_row(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByMax<T> {
        GroupByMax(row.get(name))
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByMax<T>> {
        let result: Option<T> = row.get(name);
        Some(GroupByMax(result?))
    }
}
