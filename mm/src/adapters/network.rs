use std::time::Duration;

use anyhow::{bail, Result};
use reqwest::{
    header::{HeaderMap, RETRY_AFTER},
    Response, StatusCode,
};
use tokio::{
    sync::Mutex,
    time::{sleep, Duration as TokioDuration, Instant},
};
use tracing::warn;

use crate::config::NetworkConfig;

pub struct RestGovernor {
    min_interval: Duration,
    next_allowed_at: Mutex<Instant>,
}

impl RestGovernor {
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            next_allowed_at: Mutex::new(Instant::now()),
        }
    }

    pub async fn until_ready(&self) {
        let mut next_allowed_at = self.next_allowed_at.lock().await;
        let now = Instant::now();
        if *next_allowed_at > now {
            sleep(*next_allowed_at - now).await;
        }
        *next_allowed_at = Instant::now() + self.min_interval;
    }
}

#[derive(Clone)]
pub struct RequestPolicy {
    max_attempts: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
    websocket_reconnect_backoff: Duration,
}

impl RequestPolicy {
    pub fn from_config(config: &NetworkConfig) -> Self {
        Self {
            max_attempts: config.retry_max_attempts.max(1),
            initial_backoff: Duration::from_millis(config.retry_initial_backoff_ms),
            max_backoff: Duration::from_millis(config.retry_max_backoff_ms),
            websocket_reconnect_backoff: Duration::from_millis(
                config.websocket_reconnect_backoff_ms,
            ),
        }
    }

    pub async fn send_with_retry<F, Fut>(
        &self,
        governor: &RestGovernor,
        request_name: &str,
        mut make_request: F,
    ) -> Result<Response>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = reqwest::Result<Response>>,
    {
        let mut backoff = self.initial_backoff;

        for attempt in 1..=self.max_attempts {
            governor.until_ready().await;

            let t0 = Instant::now();
            match make_request().await {
                Ok(response) => {
                    let elapsed_ms = t0.elapsed().as_millis();
                    let status = response.status();
                    if is_retryable_status(status) && attempt < self.max_attempts {
                        let delay = retry_delay(response.headers(), backoff);
                        warn!(
                            request = request_name,
                            %status,
                            attempt,
                            elapsed_ms,
                            ?delay,
                            "retryable HTTP status; backing off"
                        );
                        sleep(delay).await;
                        backoff = next_backoff(backoff, self.max_backoff);
                        continue;
                    }

                    return response.error_for_status().map_err(Into::into);
                }
                Err(err) => {
                    let elapsed_ms = t0.elapsed().as_millis();
                    if is_retryable_error(&err) && attempt < self.max_attempts {
                        warn!(
                            request = request_name,
                            attempt,
                            elapsed_ms,
                            error = %err,
                            ?backoff,
                            "transient request error; retrying"
                        );
                        sleep(backoff).await;
                        backoff = next_backoff(backoff, self.max_backoff);
                        continue;
                    }

                    return Err(err.into());
                }
            }
        }

        bail!("request exhausted retry budget: {request_name}")
    }

    pub async fn send_with_retry_allow_status<F, Fut>(
        &self,
        governor: &RestGovernor,
        request_name: &str,
        mut make_request: F,
    ) -> Result<Response>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = reqwest::Result<Response>>,
    {
        let mut backoff = self.initial_backoff;

        for attempt in 1..=self.max_attempts {
            governor.until_ready().await;

            let t0 = Instant::now();
            match make_request().await {
                Ok(response) => {
                    let elapsed_ms = t0.elapsed().as_millis();
                    let status = response.status();
                    if is_retryable_status(status) && attempt < self.max_attempts {
                        let delay = retry_delay(response.headers(), backoff);
                        warn!(
                            request = request_name,
                            %status,
                            attempt,
                            elapsed_ms,
                            ?delay,
                            "retryable HTTP status; backing off"
                        );
                        sleep(delay).await;
                        backoff = next_backoff(backoff, self.max_backoff);
                        continue;
                    }

                    return Ok(response);
                }
                Err(err) => {
                    let elapsed_ms = t0.elapsed().as_millis();
                    if is_retryable_error(&err) && attempt < self.max_attempts {
                        warn!(
                            request = request_name,
                            attempt,
                            elapsed_ms,
                            error = %err,
                            ?backoff,
                            "transient request error; retrying"
                        );
                        sleep(backoff).await;
                        backoff = next_backoff(backoff, self.max_backoff);
                        continue;
                    }

                    return Err(err.into());
                }
            }
        }

        bail!("request exhausted retry budget: {request_name}")
    }

    pub async fn run_ws_loop<F, Fut, R, RFut>(
        &self,
        stream_name: &str,
        mut run_once: F,
        mut on_reconnect: R,
    ) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
        R: FnMut() -> RFut,
        RFut: std::future::Future<Output = Result<()>>,
    {
        // Stable connection threshold: if the connection ran longer than this we
        // consider it "healthy" and reset the backoff to the base value.
        const STABLE_CONNECTION_SECS: u64 = 30;
        const WS_MAX_BACKOFF_SECS: u64 = 60;

        // Per-stream jitter: 0-3 s based on stream_name hash so concurrent streams
        // (market, trades, OB, private …) don't all reconnect simultaneously
        // (thundering herd → 58K circuit-breaker trips observed in production).
        let jitter_ms = stream_name_jitter_ms(stream_name);

        let mut current_backoff = self.websocket_reconnect_backoff;

        loop {
            let connect_start = Instant::now();
            let mut extra_delay = TokioDuration::ZERO;
            match run_once().await {
                Ok(()) => {
                    warn!(
                        stream = stream_name,
                        "websocket loop exited cleanly; reconnecting"
                    );
                }
                Err(err) => {
                    // Bug 1b: if the server returned a 429 / Too-Many-Requests during
                    // the WebSocket handshake, honour the implied back-off.  Tungstenite
                    // surfaces this as an HTTP error in the error string; we detect it
                    // heuristically and wait an extra 10 s (conservative but safe).
                    let err_str = err.to_string();
                    if err_str.contains("429")
                        || err_str.to_ascii_lowercase().contains("too many requests")
                        || err_str.to_ascii_lowercase().contains("rate limit")
                    {
                        extra_delay = TokioDuration::from_secs(10);
                        warn!(
                            stream = stream_name,
                            error = %err,
                            extra_delay_secs = 10,
                            "websocket 429 / rate-limit during handshake; applying extra back-off"
                        );
                    } else {
                        warn!(
                            stream = stream_name,
                            error = %err,
                            ?current_backoff,
                            "websocket loop failed; reconnecting"
                        );
                    }
                }
            }

            // If the connection was stable, reset backoff; otherwise double it.
            if connect_start.elapsed() >= TokioDuration::from_secs(STABLE_CONNECTION_SECS) {
                current_backoff = self.websocket_reconnect_backoff;
            } else {
                current_backoff = next_backoff(
                    current_backoff,
                    TokioDuration::from_secs(WS_MAX_BACKOFF_SECS),
                );
            }

            on_reconnect().await?;
            let total_delay = current_backoff + TokioDuration::from_millis(jitter_ms) + extra_delay;
            warn!(
                stream = stream_name,
                ?total_delay,
                "websocket reconnect back-off"
            );
            sleep(total_delay).await;
        }
    }
}

/// Derive a stable per-stream jitter (0-3000 ms) from the stream name so
/// concurrent streams don't all reconnect at the same instant.
fn stream_name_jitter_ms(name: &str) -> u64 {
    // Simple FNV-1a hash → modulo 3000 ms
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in name.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash % 3000
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
        || status == StatusCode::INTERNAL_SERVER_ERROR
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

fn retry_delay(headers: &HeaderMap, fallback: Duration) -> Duration {
    let Some(value) = headers.get(RETRY_AFTER) else {
        return fallback;
    };
    let Ok(value) = value.to_str() else {
        return fallback;
    };
    let Ok(seconds) = value.parse::<u64>() else {
        return fallback;
    };
    Duration::from_secs(seconds)
}

fn next_backoff(current: Duration, max_backoff: Duration) -> Duration {
    if current.is_zero() {
        return max_backoff.min(Duration::from_millis(100));
    }
    current.saturating_mul(2).min(max_backoff)
}
