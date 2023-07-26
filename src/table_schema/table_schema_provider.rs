use std::collections::HashMap;

use crate::ColumnName;

use super::{IndexSchema, TableColumn};

pub trait TableSchemaProvider {
    fn get_primary_key_columns() -> Option<Vec<ColumnName>>;
    fn get_columns() -> Vec<TableColumn>;
    fn get_indexes() -> Option<HashMap<String, IndexSchema>>;
}
