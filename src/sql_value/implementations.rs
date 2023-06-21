use crate::{
    sql::SqlWhereValue, SqlUpdateValueWriter, SqlValue, SqlValueMetadata, SqlWhereValueWriter,
};

pub struct RawField {
    pub value: String,
}

impl<'s> SqlWhereValueWriter<'s> for RawField {
    fn get_where_value(
        &'s self,
        _params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.value.to_string().into())
    }

    fn get_default_operator(&self) -> &'static str {
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
