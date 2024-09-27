use std::sync::Arc;

use my_ssh::*;

use super::PostgresSshConfig;
#[derive(Clone)]
pub struct SshConfigBuilder {
    pub sessions_pool: Option<Arc<SshSessionsPool>>,
    pub auth_type: SshAuthenticationType,
}

impl SshConfigBuilder {
    pub fn new() -> Self {
        Self {
            sessions_pool: None,
            auth_type: SshAuthenticationType::SshAgent,
        }
    }

    pub async fn set_cert(&mut self, private_key_content: String, pass_phrase: Option<String>) {
        self.auth_type = SshAuthenticationType::PrivateKey {
            private_key_content,
            pass_phrase,
        };
    }

    pub fn build(self, ssh_line: &str) -> PostgresSshConfig {
        let credentials = SshCredentials::try_from_str(ssh_line, self.auth_type);

        if credentials.is_none() {
            panic!("Can not create SshCredentials from ssh_line: {}", ssh_line);
        }
        PostgresSshConfig {
            credentials: Arc::new(credentials.unwrap()),
            sessions_pool: self.sessions_pool,
        }
    }
}
