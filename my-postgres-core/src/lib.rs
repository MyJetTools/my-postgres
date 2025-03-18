mod connection;
mod connection_sql_operations;
mod count_result;
mod db_row;
mod error;
mod my_postgres;
mod my_postgres_builder;
pub mod sql;
mod sql_where_value_provider;
pub use sql_where_value_provider::*;
pub mod sync_table_schema;
mod with_retries;

mod postgres_settings;
pub mod sql_insert;
pub mod sql_select;
mod sql_value;
pub use crate::my_postgres::*;

pub use connection::*;
pub use db_row::*;
pub use error::*;
pub use postgres_settings::*;
pub use sql_value::*;
pub mod sql_update;
pub mod sql_where;
pub mod table_schema;

mod update_conflict_type;
pub use my_postgres_builder::*;
pub use update_conflict_type::*;
pub use with_retries::*;
mod column_name;
pub use column_name::*;
mod group_by_fields;
pub use group_by_fields::*;
pub mod union;
pub mod utils;

mod postgres_telemetry;
#[cfg(feature = "with-ssh")]
mod ssh;

pub use postgres_telemetry::*;

pub extern crate tokio_postgres;

const POSTGRES_DEFAULT_PORT: u16 = 5432;

fn is_debug(table_name: &str, operation: &str) -> bool {
    if let Ok(debug_value) = std::env::var("DEBUG_SQL") {
        if debug_value.eq_ignore_ascii_case("true") {
            return true;
        }

        if debug_value == "1" {
            return true;
        }

        if debug_value == table_name {
            return true;
        }

        if debug_value == operation {
            return true;
        }
    }

    false
}
