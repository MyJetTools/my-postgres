use std::sync::{atomic::AtomicU16, Arc};

use my_ssh::*;
use tokio::sync::Mutex;
#[derive(Clone)]
pub struct SshTargetInner {
    pub credentials: Option<Arc<SshCredentials>>,
    pub sessions_pool: Option<Arc<SshSessionsPool>>,
}

impl SshTargetInner {
    pub async fn get_ssh_session(&self) -> Arc<SshSession> {
        if self.credentials.is_none() {
            panic!("Ssh credentials are not set")
        }

        let ssh_credentials = self.credentials.as_ref().unwrap();

        if let Some(pool) = self.sessions_pool.as_ref() {
            return pool.get_or_create(ssh_credentials).await;
        }
        let ssh_session = my_ssh::SshSession::new(ssh_credentials.clone());
        std::sync::Arc::new(ssh_session)
    }
}

#[derive(Clone)]
pub struct SshTarget {
    inner: Arc<Mutex<SshTargetInner>>,
}

impl SshTarget {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SshTargetInner {
                credentials: None,
                sessions_pool: None,
            })),
        }
    }

    pub async fn set_credentials(&self, value: Arc<SshCredentials>) {
        let mut inner = self.inner.lock().await;
        inner.credentials = Some(value);
    }

    pub async fn set_sessions_pool(&self, value: Arc<SshSessionsPool>) {
        let mut inner = self.inner.lock().await;
        inner.sessions_pool = Some(value);
    }

    pub async fn get_value(&self) -> Option<SshTargetInner> {
        let read_access = self.inner.lock().await;

        if read_access.credentials.is_none() {
            return None;
        }

        Some(read_access.clone())
    }
}

pub struct PortAllocator {
    current_port: AtomicU16,
    from: u16,
    to: u16,
}

impl PortAllocator {
    pub fn new(from: u16, to: u16) -> Self {
        Self {
            current_port: AtomicU16::new(from),
            from,
            to,
        }
    }

    pub fn get_next_port(&self) -> u16 {
        let current_port = self
            .current_port
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if current_port == self.to {
            self.current_port
                .store(self.from, std::sync::atomic::Ordering::Relaxed);
        }

        current_port
    }
}

lazy_static::lazy_static! {
    pub static ref PORT_ALLOCATOR: PortAllocator = PortAllocator::new(33000, 34000);
}

pub fn generate_unix_socket_file(
    _ssh_credentials: &SshCredentials,
    _remote_host: rust_extensions::url_utils::HostEndpoint,
) -> (&'static str, u16) {
    let port = PORT_ALLOCATOR.get_next_port();
    return ("127.0.0.1", port);
    /*
    let (ssh_host, ssh_port) = ssh_credentials.get_host_port();

    let r_host = remote_host.host;
    let r_port = match remote_host.port {
        Some(port) => port.to_string(),
        None => "".to_string(),
    };

    let root_path = match env::var("HOME") {
        Ok(value) => value,
        Err(_) => "/tmp".to_string(),
    };

    return format!("{}/postgres-connections", root_path);

    format!(
        "{}/postgres-{}-{}_{}--{}_{}.sock",
        root_path,
        ssh_credentials.get_user_name(),
        ssh_host,
        ssh_port,
        r_host,
        r_port
    )
     */
}
