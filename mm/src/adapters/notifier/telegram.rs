use std::{sync::Arc, time::Duration};

use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use tokio::{sync::Mutex, time::Instant};
use tracing::warn;

use crate::{config::TelegramConfig, ports::notifier::NotifierPort};

/// Telegram hard limits: 4096 chars per message, ~30 msgs/sec per bot.
/// We stay well under with a 40 ms minimum interval (25 msg/sec).
const MAX_MESSAGE_LEN: usize = 4096;
const MIN_SEND_INTERVAL: Duration = Duration::from_millis(40);

#[derive(Clone, Debug)]
pub enum TelegramCommand {
    Start,
    Stop,
    Restart,
}

#[derive(Clone, Default)]
pub struct TelegramNotifier {
    inner: Option<Arc<TelegramInner>>,
}

struct TelegramInner {
    client: Client,
    bot_token: String,
    chat_id: String,
    /// Token-bucket style rate limiter: tracks the earliest time the next
    /// message may be sent so we stay under Telegram's 30 msg/sec limit.
    next_send_at: Mutex<Instant>,
}

impl TelegramNotifier {
    pub fn from_config(config: Option<TelegramConfig>) -> Self {
        match config {
            Some(cfg) if cfg.enabled => Self {
                inner: Some(Arc::new(TelegramInner {
                    client: Client::new(),
                    bot_token: cfg.bot_token,
                    chat_id: cfg.chat_id,
                    next_send_at: Mutex::new(Instant::now()),
                })),
            },
            _ => Self::default(),
        }
    }

    pub async fn send(&self, message: impl Into<String>) {
        let Some(inner) = &self.inner else {
            return;
        };

        let text = message.into();
        let chunks = chunk_message(&text);
        for chunk in chunks {
            inner.send_chunk(chunk).await;
        }
    }

    pub async fn poll_commands(
        &self,
        offset: Option<i64>,
        timeout_seconds: u64,
    ) -> Result<Vec<(i64, TelegramCommand)>> {
        let Some(inner) = &self.inner else {
            return Ok(Vec::new());
        };

        let url = format!("https://api.telegram.org/bot{}/getUpdates", inner.bot_token);
        let response = inner
            .client
            .post(url)
            .json(&TelegramGetUpdatesRequest {
                offset,
                timeout: timeout_seconds,
                allowed_updates: vec!["message".to_string()],
            })
            .send()
            .await?;
        let body_text = response.text().await?;
        let body: TelegramGetUpdatesResponse = match from_str(&body_text) {
            Ok(body) => body,
            Err(err) => {
                let body_preview: String = body_text.chars().take(512).collect();
                warn!(
                    ?err,
                    body = %body_preview,
                    "telegram getUpdates returned an unparseable response"
                );
                return Ok(Vec::new());
            }
        };
        if !body.ok {
            warn!(
                error_code = ?body.error_code,
                description = ?body.description,
                "telegram getUpdates returned non-ok response"
            );
            return Ok(Vec::new());
        }

        let mut commands = Vec::new();
        let expected_chat_id = match inner.chat_id.parse::<i64>() {
            Ok(value) => value,
            Err(_) => return Ok(Vec::new()),
        };
        for update in body.result {
            let Some(message) = update.message else {
                continue;
            };
            if message.chat.id != expected_chat_id {
                continue;
            }
            let Some(text) = message.text else {
                continue;
            };
            let Some(command) = parse_command(&text) else {
                continue;
            };
            commands.push((update.update_id, command));
        }

        Ok(commands)
    }
}

#[async_trait::async_trait]
impl NotifierPort for TelegramNotifier {
    async fn notify(&self, message: String) {
        self.send(message).await;
    }
}

impl TelegramInner {
    async fn send_chunk(&self, text: String) {
        // Rate limiting: wait until next_send_at before firing.
        {
            let mut next = self.next_send_at.lock().await;
            let now = Instant::now();
            if *next > now {
                tokio::time::sleep_until(*next).await;
            }
            *next = Instant::now() + MIN_SEND_INTERVAL;
        }

        let request = TelegramMessage {
            chat_id: self.chat_id.clone(),
            text,
        };
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        match self.client.post(url).json(&request).send().await {
            Ok(response) => {
                let status = response.status();
                if !status.is_success() {
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "<failed to read body>".to_string());
                    warn!(%status, %body, "telegram alert rejected");
                }
            }
            Err(err) => {
                warn!(?err, "telegram alert failed");
            }
        }
    }
}

/// Split a message into chunks of at most MAX_MESSAGE_LEN characters,
/// breaking on newline boundaries where possible.
fn chunk_message(text: &str) -> Vec<String> {
    if text.len() <= MAX_MESSAGE_LEN {
        return vec![text.to_string()];
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let end = (start + MAX_MESSAGE_LEN).min(text.len());
        // Try to break on the last newline within the window.
        let split = if end < text.len() {
            text[start..end]
                .rfind('\n')
                .map(|pos| start + pos + 1)
                .unwrap_or(end)
        } else {
            end
        };
        chunks.push(text[start..split].to_string());
        start = split;
    }
    chunks
}

#[derive(Serialize)]
struct TelegramMessage {
    chat_id: String,
    text: String,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct TelegramGetUpdatesRequest {
    offset: Option<i64>,
    timeout: u64,
    allowed_updates: Vec<String>,
}

#[derive(Deserialize)]
struct TelegramGetUpdatesResponse {
    ok: bool,
    #[serde(default)]
    result: Vec<TelegramUpdate>,
    error_code: Option<i64>,
    description: Option<String>,
}

#[derive(Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramIncomingMessage>,
}

#[derive(Deserialize)]
struct TelegramIncomingMessage {
    text: Option<String>,
    chat: TelegramIncomingChat,
}

#[derive(Deserialize)]
struct TelegramIncomingChat {
    id: i64,
}

fn parse_command(text: &str) -> Option<TelegramCommand> {
    let command = text.split_whitespace().next()?.trim().split('@').next()?;
    match command {
        "/start" => Some(TelegramCommand::Start),
        "/stop" => Some(TelegramCommand::Stop),
        "/restart" => Some(TelegramCommand::Restart),
        _ => None,
    }
}
