use std::sync::Arc;

use my_ssh::*;

#[derive(Clone)]
pub struct PostgresSshConfig {
    pub credentials: Arc<SshCredentials>,
    pub sessions_pool: Option<Arc<SshSessionsPool>>,
}

impl PostgresSshConfig {
    pub async fn get_ssh_session(&self) -> Arc<SshSession> {
        if let Some(pool) = self.sessions_pool.as_ref() {
            return pool.get_or_create(&self.credentials).await;
        }
        let ssh_session = my_ssh::SshSession::new(self.credentials.clone());
        std::sync::Arc::new(ssh_session)
    }
}
