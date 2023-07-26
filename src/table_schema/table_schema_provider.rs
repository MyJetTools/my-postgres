use std::collections::HashMap;

use crate::ColumnName;

use super::{IndexSchema, TableColumn};

pub trait TableSchemaProvider {
    const PRIMARY_KEY_COLUMNS: Option<&'static [ColumnName]>;
    fn get_columns() -> Vec<TableColumn>;
    fn get_indexes() -> Option<HashMap<String, IndexSchema>>;
}
