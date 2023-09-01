use tokio_postgres::types::FromSql;

use crate::{
    sql::SelectBuilder,
    sql_select::{FromDbRow, SelectValueProvider},
    GroupByFieldType, SqlValueMetadata,
};

pub struct GroupByCount<T>(T);

impl<'s, T: Copy + FromSql<'s>> GroupByCount<T> {
    pub fn get_value(&self) -> T {
        self.0
    }
}

impl<'s, T: GroupByFieldType> SelectValueProvider for GroupByCount<T> {
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
            statement: format!("COUNT({field_name})::{} as field_name", sql_type).into(),
        });
    }
}

impl<'s, T: Copy + FromSql<'s>> FromDbRow<'s, GroupByCount<T>> for GroupByCount<T> {
    fn from_db_row(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> GroupByCount<T> {
        GroupByCount(row.get(name))
    }

    fn from_db_row_opt(
        row: &'s crate::DbRow,
        name: &str,
        _metadata: &Option<SqlValueMetadata>,
    ) -> Option<GroupByCount<T>> {
        let result: Option<T> = row.get(name);
        Some(GroupByCount(result?))
    }
}
