use crate::{SqlUpdateValueProvider, SqlValueMetadata};

pub struct SqlUpdateValue<'s> {
    pub metadata: Option<SqlValueMetadata>,
    pub value: Option<&'s dyn SqlUpdateValueProvider<'s>>,
}
