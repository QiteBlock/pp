use async_trait::async_trait;

#[async_trait]
pub trait NotifierPort: Send + Sync {
    async fn notify(&self, message: String);
}
