#[async_trait::async_trait]
pub trait PostgresSettings {
    async fn get_connection_string(&self) -> String;
}
