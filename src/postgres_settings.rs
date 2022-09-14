#[async_trait::async_trait]
pub trait PostgressSettings {
    async fn get_connection_string(&self) -> String;
}
