use std::sync::atomic::AtomicU16;

use my_ssh::SshCredentials;

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

pub fn generate_unix_socket_file(_ssh_credentials: &SshCredentials) -> (&'static str, u16) {
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
