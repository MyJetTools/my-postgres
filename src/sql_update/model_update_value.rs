use crate::SqlValueMetadata;

use super::SqlUpdateValueProvider;

pub struct SqlUpdateModelValue<'s> {
    pub metadata: Option<SqlValueMetadata>,
    pub value: Option<&'s dyn SqlUpdateValueProvider<'s>>,
}
