use std::collections::HashMap;

use super::{IndexSchema, TableColumn};

pub trait TableSchemaProvider {
    const PRIMARY_KEY_COLUMNS: Option<&'static [&'static str]>;
    fn get_columns() -> Vec<TableColumn>;
    fn get_indexes() -> Option<HashMap<String, IndexSchema>>;
}
