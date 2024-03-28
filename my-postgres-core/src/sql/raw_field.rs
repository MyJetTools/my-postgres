use crate::{
    sql::{SqlUpdateValue, SqlValues},
    sql_update::SqlUpdateValueProvider,
    RenderFullWhereCondition, SqlValueMetadata, SqlWhereValueProvider,
};

pub struct RawField {
    pub value: String,
}

impl SqlWhereValueProvider for RawField {
    fn fill_where_value(
        &self,
        _column_name: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        sql.push_str(&self.value);
        true
    }

    fn render_value(&self) -> bool {
        true
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
