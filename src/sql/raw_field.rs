use crate::{
    sql::{SqlUpdateValue, SqlValues, SqlWhereValue},
    sql_update::SqlUpdateValueProvider,
    SqlValueMetadata, SqlWhereValueProvider,
};

pub struct RawField {
    pub value: String,
}

impl SqlWhereValueProvider for RawField {
    fn get_where_value(
        &self,
        _params: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlWhereValue {
        SqlWhereValue::NonStringValue(self.value.as_str().into())
    }

    fn get_default_operator(&self) -> &'static str {
        "="
    }

    fn is_none(&self) -> bool {
        false
    }
}

impl SqlUpdateValueProvider for RawField {
    fn get_update_value(
        &self,
        _params: &mut SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> SqlUpdateValue {
        SqlUpdateValue::NonStringValue(self.value.as_str().into())
    }
}
