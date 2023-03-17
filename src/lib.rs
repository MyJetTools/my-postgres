mod connection;

mod count_result;
mod db_row;
mod error;
mod my_postgres;
mod postgres_settings;
pub mod sql_insert;
pub mod sql_select;
mod sql_value;

use std::sync::Arc;

pub use crate::my_postgres::*;
use crate::table_schema::TableSchemas;
pub use connection::*;

pub use error::*;
pub use postgres_settings::*;

pub use db_row::*;

pub use sql_value::*;

pub mod sql_delete;
pub mod sql_update;
pub mod sql_where;
pub mod table_schema;

lazy_static::lazy_static! {
    pub static ref TABLE_SCHEMAS: Arc<TableSchemas> = {
        Arc::new(TableSchemas::new())
    };

}
