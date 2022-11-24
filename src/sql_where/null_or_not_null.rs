use crate::SqlValueWriter;

pub struct IsNull {
    value: bool,
}

impl IsNull {
    pub fn new(value: bool) -> Self {
        Self { value }
    }
}

impl<'s> SqlValueWriter<'s> for IsNull {
    fn write(
        &'s self,
        sql: &mut String,
        _params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        if self.value {
            sql.push_str("IS NULL");
        } else {
            sql.push_str("IS NOT NULL");
        }
    }
}
