use crate::SqlValueWriter;

pub struct SelectRawField {
    pub value: String,
}

impl<'s> SqlValueWriter<'s> for SelectRawField {
    fn write(
        &'s self,
        sql: &mut String,
        _params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.value.as_str());
    }
}
