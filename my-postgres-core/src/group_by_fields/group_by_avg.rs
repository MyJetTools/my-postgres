use tokio_postgres::types::FromSql;

use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    GroupByFieldType, SqlValueMetadata,
};

#[derive(Debug)]
pub struct GroupByAvg<T: std::fmt::Debug + Send + Sync + 'static>(T);

impl<'s, T: std::fmt::Debug + Copy + FromSql<'s> + Send + Sync + 'static> GroupByAvg<T> {
    pub fn get_value(&self) -> T {
        self.0
    }
}

impl<T: std::fmt::Debug + GroupByFieldType + Send + Sync + 'static> SelectValueProvider
    for GroupByAvg<T>
{
    fn fill_select_part(
        sql: &mut SelectBuilder,
        field_name: &'static str,
        db_column_name: &'static str,
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
            statement: format!("AVG({db_column_name})::{}", sql_type).into(),
            db_column_name,
        });
    }
}

impl<'s, T: std::fmt::Debug + Copy + FromSql<'s> + Send + Sync + 'static>
    FromDbRow<'s, GroupByAvg<T>> for GroupByAvg<T>
{
    fn from_db_row(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByAvg<T> {
        let value: T = row.get(name);
        GroupByAvg(value)
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByAvg<T>> {
        let result: Option<T> = row.get(name);
        Some(GroupByAvg(result?))
    }
}
