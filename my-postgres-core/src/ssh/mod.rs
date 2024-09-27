mod ssh_config_builder;
use std::collections::HashMap;

pub use ssh_config_builder::*;
mod ssh_config;
pub use ssh_config::*;
mod port_allocator;
pub use port_allocator::*;
mod start_ssh_tunnel;
pub use start_ssh_tunnel::*;
use tokio::sync::Mutex;

lazy_static::lazy_static! {
    pub static ref ESTABLISHED_TUNNELS: Mutex<HashMap<String, (String, u16)>> = Mutex::new(HashMap::new());
}
