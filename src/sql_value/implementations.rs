use crate::{SqlUpdateValueWriter, SqlValue, SqlValueMetadata, SqlWhereValueWriter};

pub struct RawField {
    pub value: String,
}

impl<'s> SqlWhereValueWriter<'s> for RawField {
    fn write(
        &'s self,
        sql: &mut String,
        _params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.value.as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlUpdateValueWriter<'s> for RawField {
    fn write(
        &'s self,
        sql: &mut String,
        _params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.value.as_str());
    }
}
