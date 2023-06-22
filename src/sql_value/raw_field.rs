use crate::{
    sql::{SqlUpdateValue, SqlValues, SqlWhereValue},
    sql_update::SqlUpdateValueProvider,
    SqlValueMetadata, SqlWhereValueProvider,
};

pub struct RawField {
    pub value: String,
}

impl<'s> SqlWhereValueProvider<'s> for RawField {
    fn get_where_value(
        &'s self,
        _params: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue<'s> {
        SqlWhereValue::NonStringValue(self.value.as_str().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl<'s> SqlUpdateValueProvider<'s> for RawField {
    fn get_update_value(
        &'s self,
        _params: &mut SqlValues<'s>,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue<'s> {
        SqlUpdateValue::NonStringValue(self.value.as_str().into())
    }
}
