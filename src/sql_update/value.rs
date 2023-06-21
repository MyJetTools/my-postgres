use crate::{SqlUpdateValueWriter, SqlValueMetadata};

pub struct SqlUpdateValue<'s> {
    pub metadata: Option<SqlValueMetadata>,
    pub value: Option<&'s dyn SqlUpdateValueWriter<'s>>,
}
