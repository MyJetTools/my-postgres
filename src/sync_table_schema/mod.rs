mod sync;
pub use sync::*;
mod sync_indexes;
mod sync_primary_key;
mod sync_table_fields;
mod update_column;
pub use sync_indexes::*;
mod check_if_db_exists;
use sync_primary_key::*;
pub use sync_table_fields::*;
pub use update_column::*;
pub const TABLE_SCHEMA_SYNCHRONIZATION: &'static str = "Table Schema Synchronization";
pub use check_if_db_exists::*;
pub const SCHEMA_SYNC_SQL_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
