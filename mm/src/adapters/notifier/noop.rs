use crate::ports::notifier::NotifierPort;

#[derive(Clone, Default)]
pub struct NoopNotifier;

impl NoopNotifier {
    pub async fn send(&self, _message: impl Into<String>) {}
}

#[async_trait::async_trait]
impl NotifierPort for NoopNotifier {
    async fn notify(&self, message: String) {
        self.send(message).await;
    }
}
