use crate::SqlValueWriter;

impl<'s> SqlValueWriter<'s> for tokio_postgres::types::IsNull {
    fn write(
        &'s self,
        sql: &mut String,
        _params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _sql_type: Option<&'static str>,
    ) {
        match self {
            tokio_postgres::types::IsNull::Yes => {
                sql.push_str("IS NULL");
            }
            tokio_postgres::types::IsNull::No => {
                sql.push_str("IS NOT NULL");
            }
        }
    }
}
