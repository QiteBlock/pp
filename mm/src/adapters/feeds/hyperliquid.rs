use std::{collections::HashMap, str::FromStr, time::Duration};

use anyhow::Result;
use chrono::TimeZone;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use tracing::warn;

use crate::domain::{ExternalVenue, MarketEvent};

const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const RECONNECT_DELAY_SECS: u64 = 2;

pub async fn stream_hyperliquid_bbo(
    symbol_map: HashMap<String, String>,
    sender: mpsc::Sender<MarketEvent>,
) -> Result<()> {
    if symbol_map.is_empty() {
        std::future::pending::<()>().await;
        return Ok(());
    }

    let reverse_map: HashMap<String, String> = symbol_map
        .iter()
        .map(|(internal_symbol, hl_coin)| (hl_coin.clone(), internal_symbol.clone()))
        .collect();
    let subscriptions: Vec<String> = symbol_map.values().cloned().collect();

    loop {
        match connect_async(HYPERLIQUID_WS_URL).await {
            Ok((mut ws_stream, _)) => {
                let mut subscribe_failed = false;
                for coin in &subscriptions {
                    let subscribe = json!({
                        "method": "subscribe",
                        "subscription": {
                            "type": "bbo",
                            "coin": coin,
                        }
                    })
                    .to_string();
                    if let Err(err) = ws_stream.send(WsMessage::Text(subscribe.into())).await {
                        warn!(coin = %coin, err = %err, "hyperliquid subscribe failed");
                        subscribe_failed = true;
                        break;
                    }
                }
                if subscribe_failed {
                    tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
                    continue;
                }

                loop {
                    match ws_stream.next().await {
                        Some(Ok(WsMessage::Text(text))) => {
                            if let Some(event) = parse_hyperliquid_bbo(&text, &reverse_map) {
                                let _ = sender.try_send(event);
                            }
                        }
                        Some(Ok(WsMessage::Ping(payload))) => {
                            if let Err(err) = ws_stream.send(WsMessage::Pong(payload)).await {
                                warn!(err = %err, "hyperliquid pong failed; reconnecting");
                                break;
                            }
                        }
                        Some(Ok(WsMessage::Close(_))) => {
                            warn!("hyperliquid websocket closed; reconnecting");
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(err)) => {
                            warn!(err = %err, "hyperliquid websocket error; reconnecting");
                            break;
                        }
                        None => {
                            warn!("hyperliquid websocket ended; reconnecting");
                            break;
                        }
                    }
                }
            }
            Err(err) => warn!(err = %err, "hyperliquid websocket connect failed; retrying"),
        }

        tokio::time::sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }
}

fn parse_hyperliquid_bbo(text: &str, reverse_map: &HashMap<String, String>) -> Option<MarketEvent> {
    let value: Value = serde_json::from_str(text).ok()?;
    if value.get("channel").and_then(Value::as_str)? != "bbo" {
        return None;
    }
    let data = value.get("data")?;
    let coin = data.get("coin").and_then(Value::as_str)?;
    let symbol = reverse_map.get(coin)?.clone();
    let (bid_px, bid_sz, ask_px, ask_sz) = parse_bbo_levels(data.get("bbo")?)?;
    if bid_px <= Decimal::ZERO || ask_px <= Decimal::ZERO {
        return None;
    }
    let timestamp = chrono::Utc
        .timestamp_millis_opt(
            data.get("time")
                .or_else(|| data.get("t"))
                .and_then(Value::as_i64)?,
        )
        .single()?;
    Some(MarketEvent::ExternalBestBidAsk {
        symbol,
        venue: ExternalVenue::Hyperliquid,
        bid: bid_px,
        ask: ask_px,
        bid_size: bid_sz,
        ask_size: ask_sz,
        timestamp,
    })
}

fn parse_bbo_levels(value: &Value) -> Option<(Decimal, Option<Decimal>, Decimal, Option<Decimal>)> {
    if let Some(levels) = value.as_array() {
        let bid = levels.first()?;
        let ask = levels.get(1)?;
        return Some((
            bid.get("px").and_then(as_decimal)?,
            bid.get("sz").and_then(as_decimal),
            ask.get("px").and_then(as_decimal)?,
            ask.get("sz").and_then(as_decimal),
        ));
    }
    let bid = value.get("bid").or_else(|| value.get("b"))?;
    let ask = value.get("ask").or_else(|| value.get("a"))?;
    Some((
        bid.get("px")
            .or_else(|| bid.get("price"))
            .and_then(as_decimal)?,
        bid.get("sz")
            .or_else(|| bid.get("size"))
            .and_then(as_decimal),
        ask.get("px")
            .or_else(|| ask.get("price"))
            .and_then(as_decimal)?,
        ask.get("sz")
            .or_else(|| ask.get("size"))
            .and_then(as_decimal),
    ))
}

fn as_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str(text).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}
