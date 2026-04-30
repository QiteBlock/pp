use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, USER_AGENT},
    Client,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use secp256k1::{ecdsa::RecoverableSignature, Message, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message as WsMessage},
};
use tracing::warn;

use crate::{
    adapters::{
        exchange::{ExchangeClient, MarketDataSource, OrderExecutor, PrivateDataSource},
        network::{RequestPolicy, RestGovernor},
    },
    config::{HibachiAuthMode, NetworkConfig, VenueConfig},
    domain::{
        Fill, InstrumentMeta, MarketEvent, OpenOrder, OrderRequest, OrderType, Position,
        PrivateEvent, Side,
    },
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct HibachiClient {
    config: VenueConfig,
    http: Client,
    rest_governor: Arc<RestGovernor>,
    request_policy: RequestPolicy,
    instrument_cache: Arc<RwLock<Option<HashMap<String, InstrumentMeta>>>>,
}

impl HibachiClient {
    pub fn new(config: VenueConfig, network: &NetworkConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&config.api_key).context("invalid hibachi api key header")?,
        );
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&config.user_agent).context("invalid user-agent")?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let http = Client::builder().default_headers(headers).build()?;
        Ok(Self {
            config,
            http,
            rest_governor: Arc::new(RestGovernor::new(Duration::from_millis(
                network.private_rest_min_interval_ms,
            ))),
            request_policy: RequestPolicy::from_config(network),
            instrument_cache: Arc::new(RwLock::new(None)),
        })
    }

    fn now_nonce(&self) -> Result<u64> {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before unix epoch")?
            .as_micros();
        Ok(u64::try_from(micros).context("nonce overflow")?)
    }

    fn sign_u64(&self, value: u64) -> Result<String> {
        self.sign_bytes(&value.to_be_bytes())
    }

    fn sign_order_id(&self, order_id: &str) -> Result<String> {
        let order_id = order_id
            .parse::<u64>()
            .with_context(|| format!("invalid hibachi order id: {order_id}"))?;
        self.sign_u64(order_id)
    }

    fn sign_order(
        &self,
        request: &OrderRequest,
        instrument: &InstrumentMeta,
        nonce: u64,
    ) -> Result<String> {
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&nonce.to_be_bytes());
        payload.extend_from_slice(&instrument.contract_id.to_be_bytes());
        payload.extend_from_slice(
            &to_exchange_quantity(request.quantity, instrument.underlying_decimals)?.to_be_bytes(),
        );
        payload.extend_from_slice(
            &(match request.side {
                Side::Ask => 0u32,
                Side::Bid => 1u32,
            })
            .to_be_bytes(),
        );
        if let Some(price) = request.price {
            let price = to_exchange_price(price, instrument, &self.config)?;
            payload.extend_from_slice(&price.to_be_bytes());
        }
        let max_fee_rate = Decimal::from_str(&self.config.max_fee_rate)?;
        let max_fee = to_rate_precision(max_fee_rate, 8)?;
        payload.extend_from_slice(&max_fee.to_be_bytes());
        self.sign_bytes(&payload)
    }

    fn sign_bytes(&self, payload: &[u8]) -> Result<String> {
        match self.config.auth_mode {
            HibachiAuthMode::ExchangeManaged => {
                let mut mac = HmacSha256::new_from_slice(
                    normalize_hex(&self.config.private_key)?.as_slice(),
                )?;
                mac.update(payload);
                Ok(hex::encode(mac.finalize().into_bytes()))
            }
            HibachiAuthMode::Trustless => {
                let secret_bytes = normalize_hex(&self.config.private_key)?;
                let secret = SecretKey::from_byte_array(
                    secret_bytes
                        .try_into()
                        .map_err(|_| anyhow!("invalid secret key length"))?,
                )?;
                let digest: [u8; 32] = Sha256::digest(payload).into();
                let message = Message::from_digest(digest);
                let secp = Secp256k1::new();
                let signature = secp.sign_ecdsa_recoverable(message, &secret);
                Ok(encode_recoverable_signature(signature))
            }
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.api_base_url, path)
    }

    fn data_endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.data_api_base_url, path)
    }

    async fn load_instruments_uncached(&self) -> Result<Vec<InstrumentMeta>> {
        let response: ExchangeInfoResponse = self
            .request_policy
            .send_with_retry(&self.rest_governor, "load_instruments", || {
                self.http
                    .get(self.data_endpoint("/market/exchange-info"))
                    .send()
            })
            .await?
            .json()
            .await?;

        response
            .future_contracts
            .into_iter()
            .map(|contract| {
                Ok(InstrumentMeta {
                    symbol: contract.symbol,
                    contract_id: contract.id,
                    underlying_decimals: contract.underlying_decimals,
                    settlement_decimals: contract.settlement_decimals,
                    min_order_size: contract
                        .min_order_size
                        .unwrap_or_else(|| Decimal::new(1, 0)),
                    tick_size: contract.tick_size,
                })
            })
            .collect()
    }

    async fn instruments_by_symbol(&self) -> Result<HashMap<String, InstrumentMeta>> {
        if let Some(cache) = self.instrument_cache.read().await.as_ref() {
            return Ok(cache.clone());
        }

        let instruments = self.load_instruments_uncached().await?;
        let by_symbol: HashMap<String, InstrumentMeta> = instruments
            .into_iter()
            .map(|instrument| (instrument.symbol.clone(), instrument))
            .collect();

        *self.instrument_cache.write().await = Some(by_symbol.clone());
        Ok(by_symbol)
    }

    async fn fetch_account_snapshot(&self) -> Result<AccountSnapshot> {
        let url = format!(
            "{}?accountId={}",
            self.config.ws_account_url, self.config.account_id
        );
        let mut request = url.into_client_request()?;
        request
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_str(&self.config.api_key)?);
        request
            .headers_mut()
            .insert(USER_AGENT, HeaderValue::from_str(&self.config.user_agent)?);

        let (mut ws_stream, _) = connect_async(request).await?;
        let start = serde_json::to_string(&serde_json::json!({
            "id": 1u64,
            "method": "stream.start",
            "params": {
                "accountId": self.config.account_id
            }
        }))?;
        ws_stream.send(WsMessage::Text(start.into())).await?;

        let mut snapshot = None;
        while let Some(message) = ws_stream.next().await {
            match message? {
                WsMessage::Text(text) => {
                    if let Some(parsed) = parse_account_snapshot(&text) {
                        snapshot = Some(parsed);
                        break;
                    }
                }
                WsMessage::Ping(payload) => {
                    ws_stream.send(WsMessage::Pong(payload)).await?;
                }
                WsMessage::Close(frame) => {
                    bail!("hibachi account snapshot websocket closed: {frame:?}");
                }
                _ => {}
            }
        }

        let _ = ws_stream.close(None).await;
        snapshot.context("account snapshot missing from private websocket")
    }

    async fn stream_market_topic(
        &self,
        stream_name: &'static str,
        topic: &'static str,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.request_policy
            .run_ws_loop(
                stream_name,
                || {
                    let sender = sender.clone();
                    async move {
                        let (mut ws_stream, _) = connect_async(&self.config.ws_market_url).await?;
                        let subscriptions: Vec<WsSubscription> = symbols
                            .iter()
                            .map(|symbol| WsSubscription::new(symbol, topic))
                            .collect();

                        let subscribe = serde_json::to_string(&serde_json::json!({
                            "id": market_ws_request_id(stream_name),
                            "method": "subscribe",
                            "parameters": {
                                "subscriptions": subscriptions
                            }
                        }))?;
                        ws_stream.send(WsMessage::Text(subscribe.into())).await?;

                        while let Some(message) = ws_stream.next().await {
                            let message = message?;
                            match message {
                                WsMessage::Text(text) => {
                                    let events = Self::parse_market_events(&text)?;
                                    for event in events {
                                        sender.send(event).await?;
                                    }
                                }
                                WsMessage::Ping(payload) => {
                                    ws_stream.send(WsMessage::Pong(payload)).await?;
                                }
                                WsMessage::Close(frame) => {
                                    warn!(
                                        stream = stream_name,
                                        ?frame,
                                        "market websocket closed by peer"
                                    );
                                    break;
                                }
                                _other => {}
                            }
                        }

                        bail!("hibachi market websocket closed")
                    }
                },
                || {
                    let sender = sender.clone();
                    let symbols = symbols.to_vec();
                    async move {
                        sender
                            .send(MarketEvent::StreamReconnected { symbols })
                            .await
                            .map_err(Into::into)
                    }
                },
            )
            .await
    }

    fn parse_market_events(frame: &str) -> Result<Vec<MarketEvent>> {
        let value: Value = serde_json::from_str(frame)?;
        let mut output = Vec::new();
        match &value {
            Value::Array(events) => {
                for event in events {
                    Self::parse_market_event_value(event, &mut output)?;
                }
            }
            Value::Object(_) => Self::parse_market_event_value(&value, &mut output)?,
            _ => {}
        }

        Ok(output)
    }

    fn parse_market_event_value(event: &Value, output: &mut Vec<MarketEvent>) -> Result<()> {
        let symbol = event
            .get("symbol")
            .or_else(|| event.get("params").and_then(|params| params.get("symbol")))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let topic = event
            .get("topic")
            .or_else(|| event.get("params").and_then(|params| params.get("topic")))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if symbol.is_empty() || topic.is_empty() {
            return Ok(());
        }

        let data = event
            .get("data")
            .or_else(|| event.get("params").and_then(|params| params.get("data")))
            .unwrap_or(&Value::Null);
        let timestamp = parse_event_timestamp(event)
            .or_else(|| parse_event_timestamp(data))
            .unwrap_or_else(Utc::now);

        match topic {
            "mark_price" => {
                output.push(MarketEvent::MarkPrice {
                    symbol,
                    price: parse_decimal(&data["markPrice"])?,
                    timestamp,
                });
            }
            "spot_price" => {
                output.push(MarketEvent::SpotPrice {
                    symbol,
                    price: parse_decimal(&data["spotPrice"])?,
                    timestamp,
                });
            }
            "ask_bid_price" => {
                output.push(MarketEvent::BestBidAsk {
                    symbol,
                    bid: parse_decimal(&data["bidPrice"])?,
                    ask: parse_decimal(&data["askPrice"])?,
                    bid_size: None,
                    ask_size: None,
                    timestamp,
                });
            }
            "trades" => {
                let trade = data.get("trade").unwrap_or(data);
                let ts = parse_event_timestamp(trade)
                    .or_else(|| parse_event_timestamp(data))
                    .or_else(|| parse_event_timestamp(event))
                    .unwrap_or(timestamp);
                output.push(MarketEvent::Trade {
                    symbol,
                    price: parse_decimal(&trade["price"])?,
                    quantity: parse_decimal(&trade["quantity"])?,
                    taker_side: match trade.get("takerSide").and_then(Value::as_str) {
                        Some("Buy") => Some(Side::Bid),
                        Some("Sell") => Some(Side::Ask),
                        _ => None,
                    },
                    timestamp: ts,
                });
            }
            "orderbook" => {
                let message_type = event
                    .get("messageType")
                    .or_else(|| data.get("messageType"))
                    .and_then(Value::as_str)
                    .unwrap_or("Update");
                let bids = parse_book_side(&data["bid"])?;
                let asks = parse_book_side(&data["ask"])?;
                output.push(if message_type.eq_ignore_ascii_case("Snapshot") {
                    MarketEvent::OrderBookSnapshot {
                        symbol,
                        bids,
                        asks,
                        timestamp,
                    }
                } else {
                    MarketEvent::OrderBookUpdate {
                        symbol,
                        bids,
                        asks,
                        timestamp,
                    }
                });
            }
            _ => {}
        }

        Ok(())
    }

    fn parse_private_events(frame: &str) -> Vec<PrivateEvent> {
        let mut events = Vec::new();
        let Ok(value) = serde_json::from_str::<Value>(frame) else {
            return events;
        };

        if let Some(event_name) = value.get("event").and_then(Value::as_str) {
            let data = value.get("data").unwrap_or(&Value::Null);
            match event_name {
                "order_creation" | "order_update" | "order_created" | "order_updated" => {
                    if let Some(order) = parse_open_order(data) {
                        events.push(PrivateEvent::UpsertOpenOrder(order));
                    }
                }
                "order_cancellation" | "order_cancelled" | "order_canceled" => {
                    events.push(PrivateEvent::RemoveOpenOrder {
                        order_id: data
                            .get("orderId")
                            .and_then(Value::as_str)
                            .map(ToOwned::to_owned),
                        nonce: data.get("nonce").and_then(Value::as_u64),
                    });
                }
                "trade_update" | "trade" | "fill" | "fill_update" => {
                    if let Some(fill) = parse_fill(data) {
                        events.push(PrivateEvent::Fill(fill));
                    }
                }
                "position_update" => {
                    if let Some(position) = parse_position(data) {
                        events.push(PrivateEvent::Position(position));
                    }
                }
                "balance_update" => {
                    if let Some(equity) = data
                        .get("updatedCollateralBalance")
                        .or_else(|| data.get("equity"))
                        .and_then(as_decimal)
                    {
                        events.push(PrivateEvent::AccountEquity { equity });
                    }
                }
                _ => {}
            }
        }

        if let Some(equity) = value.get("equity").and_then(as_decimal) {
            events.push(PrivateEvent::AccountEquity { equity });
        }

        if let Some(items) = value.get("fills").and_then(Value::as_array) {
            for item in items {
                if let Some(fill) = parse_fill(item) {
                    events.push(PrivateEvent::Fill(fill));
                }
            }
        }

        if let Some(items) = value.get("positions").and_then(Value::as_array) {
            for item in items {
                if let Some(position) = parse_position(item) {
                    events.push(PrivateEvent::Position(position));
                }
            }
        }

        if let Some(items) = value
            .get("openOrders")
            .or_else(|| value.get("orders"))
            .and_then(Value::as_array)
        {
            let orders = items
                .iter()
                .filter_map(parse_open_order)
                .collect::<Vec<_>>();
            if !orders.is_empty() {
                events.push(PrivateEvent::OpenOrders(orders));
            }
        }

        events
    }
}

#[async_trait]
impl ExchangeClient for HibachiClient {
    async fn load_instruments(&self) -> Result<Vec<InstrumentMeta>> {
        Ok(self.instruments_by_symbol().await?.into_values().collect())
    }
}

#[async_trait]
impl MarketDataSource for HibachiClient {
    async fn stream_mark_prices(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.stream_market_topic("mark_price", "mark_price", symbols, sender)
            .await
    }

    async fn stream_spot_prices(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.stream_market_topic("spot_price", "spot_price", symbols, sender)
            .await
    }

    async fn stream_best_bid_ask(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.stream_market_topic("ask_bid_price", "ask_bid_price", symbols, sender)
            .await
    }

    async fn stream_trades(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.stream_market_topic("trades", "trades", symbols, sender)
            .await
    }

    async fn stream_orderbook(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.stream_market_topic("orderbook", "orderbook", symbols, sender)
            .await
    }
}

#[async_trait]
impl PrivateDataSource for HibachiClient {
    async fn stream_private_data(&self, sender: mpsc::Sender<PrivateEvent>) -> Result<()> {
        self.request_policy
            .run_ws_loop(
                "private",
                || {
                    let sender = sender.clone();
                    async move {
                        let url = format!(
                            "{}?accountId={}",
                            self.config.ws_account_url, self.config.account_id
                        );
                        let mut request = url.into_client_request()?;
                        request
                            .headers_mut()
                            .insert(AUTHORIZATION, HeaderValue::from_str(&self.config.api_key)?);
                        request
                            .headers_mut()
                            .insert(USER_AGENT, HeaderValue::from_str(&self.config.user_agent)?);

                        let (mut ws_stream, _) = connect_async(request).await?;
                        let start = serde_json::to_string(&serde_json::json!({
                            "id": 1u64,
                            "method": "stream.start",
                            "params": {
                                "accountId": self.config.account_id
                            }
                        }))?;
                        ws_stream.send(WsMessage::Text(start.into())).await?;

                        let listen_key = loop {
                            let Some(message) = ws_stream.next().await else {
                                bail!(
                                    "hibachi private websocket closed before stream.start response"
                                );
                            };
                            match message? {
                                WsMessage::Text(text) => {
                                    if let Some(listen_key) = extract_listen_key(&text) {
                                        break listen_key;
                                    }
                                    let events = Self::parse_private_events(&text);
                                    if !events.is_empty() {
                                        for event in events {
                                            sender.send(event).await?;
                                        }
                                        continue;
                                    }
                                }
                                WsMessage::Ping(payload) => {
                                    ws_stream.send(WsMessage::Pong(payload)).await?;
                                }
                                WsMessage::Close(frame) => {
                                    warn!(
                                        ?frame,
                                        "private websocket closed by peer during startup"
                                    );
                                    bail!("hibachi private websocket closed during startup");
                                }
                                _other => {}
                            }
                        };

                        let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
                        loop {
                            tokio::select! {
                                _ = ping_interval.tick() => {
                                    let ping = serde_json::to_string(&serde_json::json!({
                                        "id": 2u64,
                                        "method": "stream.ping",
                                        "params": {
                                            "accountId": self.config.account_id,
                                            "listenKey": listen_key
                                        }
                                    }))?;
                                    ws_stream.send(WsMessage::Text(ping.into())).await?;
                                }
                                maybe_message = ws_stream.next() => {
                                    let Some(message) = maybe_message else {
                                        break;
                                    };
                                    match message? {
                                        WsMessage::Text(text) => {
                                            let events = Self::parse_private_events(&text);
                                            if events.is_empty() {
                                                continue;
                                            }
                                            for event in events {
                                                sender.send(event).await?;
                                            }
                                        }
                                        WsMessage::Ping(payload) => {
                                            ws_stream.send(WsMessage::Pong(payload)).await?;
                                        }
                                        WsMessage::Close(frame) => {
                                            warn!(?frame, "private websocket closed by peer");
                                            break;
                                        }
                                        _other => {}
                                    }
                                }
                            }
                        }

                        bail!("hibachi private websocket closed")
                    }
                },
                || {
                    let sender = sender.clone();
                    async move {
                        sender
                            .send(PrivateEvent::StreamReconnected)
                            .await
                            .map_err(Into::into)
                    }
                },
            )
            .await
    }

    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>> {
        let response: Vec<PendingOrder> = self
            .request_policy
            .send_with_retry(&self.rest_governor, "fetch_open_orders", || {
                self.http
                    .get(self.endpoint(&format!(
                        "/trade/orders?accountId={}",
                        self.config.account_id
                    )))
                    .send()
            })
            .await?
            .json()
            .await?;

        response
            .into_iter()
            .map(|order| {
                Ok(OpenOrder {
                    order_id: order.order_id,
                    nonce: order.nonce.unwrap_or_default(),
                    level_index: None,
                    symbol: order.symbol,
                    side: order.side,
                    price: order.price,
                    remaining_quantity: order.available_quantity.unwrap_or(order.total_quantity),
                })
            })
            .collect()
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>> {
        Ok(self.fetch_account_snapshot().await?.positions)
    }
}

#[async_trait]
impl OrderExecutor for HibachiClient {
    async fn place_orders(&self, requests: Vec<OrderRequest>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }

        let instrument_by_symbol = self.instruments_by_symbol().await?;

        let mut payload_orders = Vec::with_capacity(requests.len());
        for request in requests {
            let instrument = instrument_by_symbol
                .get(&request.symbol)
                .with_context(|| format!("missing instrument metadata for {}", request.symbol))?;
            let nonce = self.now_nonce()?;
            let signature = self.sign_order(&request, instrument, nonce)?;
            payload_orders.push(CreateOrderPayload {
                action: "place".to_string(),
                creation_deadline: None,
                max_fees_percent: self.config.max_fee_rate.clone(),
                nonce,
                order_flags: if request.post_only {
                    Some("POST_ONLY".to_string())
                } else {
                    None
                },
                order_type: request.order_type,
                price: request.price,
                quantity: request.quantity,
                side: request.side,
                signature,
                symbol: request.symbol,
                trigger_price: None,
            });
            tokio::time::sleep(Duration::from_micros(25)).await;
        }

        let body = CreateOrdersRequest {
            account_id: self.config.account_id,
            orders: payload_orders,
        };

        self.rest_governor.until_ready().await;
        let response = self
            .http
            .post(self.endpoint("/trade/orders"))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let response_text = response.text().await?;
        if !status.is_success() {
            bail!("place_orders failed with HTTP {status}: {response_text}");
        }
        parse_batch_results(&response_text)?.validate("place")?;
        Ok(())
    }

    async fn cancel_orders(&self, order_ids: Vec<String>) -> Result<()> {
        if order_ids.is_empty() {
            return Ok(());
        }

        let mut orders = Vec::with_capacity(order_ids.len());
        for order_id in order_ids {
            let signature = self.sign_order_id(&order_id)?;
            orders.push(CancelOrderPayload {
                action: "cancel".to_string(),
                order_id,
                signature,
            });
        }

        let body = CancelOrdersBatchRequest {
            account_id: self.config.account_id,
            orders,
        };

        self.rest_governor.until_ready().await;
        let response = self
            .http
            .post(self.endpoint("/trade/orders"))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let response_text = response.text().await?;
        if !status.is_success() {
            bail!("cancel_orders failed with HTTP {status}: {response_text}");
        }
        parse_batch_results(&response_text)?.validate("cancel")?;
        Ok(())
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        let nonce = self.now_nonce()?;
        let signature = self.sign_u64(nonce)?;

        let body = CancelAllOrdersRequest {
            account_id: self.config.account_id,
            nonce,
            signature,
        };

        self.rest_governor.until_ready().await;
        let response = self
            .http
            .delete(self.endpoint("/trade/orders"))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let response_text = response.text().await?;
        if !status.is_success() {
            bail!("cancel_all_orders failed with HTTP {status}: {response_text}");
        }
        Ok(())
    }
}

fn parse_decimal(value: &Value) -> Result<Decimal> {
    match value {
        Value::String(value) => Ok(Decimal::from_str(value)?),
        Value::Number(value) => Decimal::from_str(&value.to_string()).map_err(Into::into),
        _ => Err(anyhow!("expected decimal-compatible value")),
    }
}

fn as_decimal(value: &Value) -> Option<Decimal> {
    parse_decimal(value).ok()
}

fn parse_fill(value: &Value) -> Option<Fill> {
    Some(Fill {
        order_id: value
            .get("orderId")
            .or_else(|| value.get("order_id"))
            .and_then(Value::as_str)
            .map(ToString::to_string),
        nonce: value
            .get("nonce")
            .or_else(|| value.get("n"))
            .and_then(Value::as_u64),
        symbol: value.get("symbol")?.as_str()?.to_string(),
        side: parse_fill_side(value)?,
        price: value
            .get("price")
            .or_else(|| value.get("matchPrice"))
            .or_else(|| value.get("executionPrice"))
            .and_then(as_decimal)?,
        quantity: value
            .get("quantity")
            .or_else(|| value.get("matchedQuantity"))
            .or_else(|| value.get("executedQuantity"))
            .and_then(as_decimal)?,
        timestamp: parse_timestamp(
            value
                .get("timestamp")
                .or_else(|| value.get("tradeTime"))
                .or_else(|| value.get("eventTime")),
        )
        .unwrap_or_else(Utc::now),
    })
}

fn parse_position(value: &Value) -> Option<Position> {
    let payload = value.get("updatedPosition").unwrap_or(value);
    let mut quantity = as_decimal(
        payload
            .get("quantity")
            .or_else(|| payload.get("position"))?,
    )?;
    if let Some(direction) = payload.get("direction").and_then(Value::as_str) {
        match direction {
            "Short" | "short" | "SHORT" => quantity = -quantity.abs(),
            "Long" | "long" | "LONG" => quantity = quantity.abs(),
            _ => {}
        }
    }
    let entry_price = payload
        .get("entryPrice")
        .or_else(|| payload.get("entry_price"))
        .or_else(|| payload.get("openPrice"))
        .and_then(as_decimal)
        .or_else(|| {
            let entry_notional = payload.get("entryNotional").and_then(as_decimal)?;
            let abs_quantity = quantity.abs();
            if abs_quantity.is_zero() {
                None
            } else {
                Some(entry_notional / abs_quantity)
            }
        })
        .unwrap_or(Decimal::ZERO);
    let unrealized_pnl = payload
        .get("unrealizedPnl")
        .or_else(|| payload.get("unrealized_pnl"))
        .and_then(as_decimal)
        .or_else(|| {
            let funding = payload.get("unrealizedFundingPnl").and_then(as_decimal)?;
            let trading = payload.get("unrealizedTradingPnl").and_then(as_decimal)?;
            Some(funding + trading)
        })
        .unwrap_or(Decimal::ZERO);
    Some(Position {
        symbol: value.get("symbol")?.as_str()?.to_string(),
        quantity,
        entry_price,
        realized_pnl: payload
            .get("realizedPnl")
            .or_else(|| payload.get("realized_pnl"))
            .or_else(|| payload.get("realizedTradingPnl"))
            .and_then(as_decimal)
            .unwrap_or(Decimal::ZERO),
        unrealized_pnl,
        // Only authoritative when the venue actually sends a non-zero realized_pnl.
        // When the field is absent/zero the engine's fill-math running total wins.
        pnl_is_authoritative: payload
            .get("realizedPnl")
            .or_else(|| payload.get("realized_pnl"))
            .or_else(|| payload.get("realizedTradingPnl"))
            .and_then(as_decimal)
            .map(|pnl| !pnl.is_zero())
            .unwrap_or(false),
        opened_at: None,
    })
}

fn parse_open_order(value: &Value) -> Option<OpenOrder> {
    Some(OpenOrder {
        order_id: value
            .get("orderId")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        nonce: value
            .get("nonce")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        level_index: None,
        symbol: value.get("symbol")?.as_str()?.to_string(),
        side: parse_side(value.get("side")?)?,
        price: value.get("price").and_then(as_decimal),
        remaining_quantity: value
            .get("availableQuantity")
            .or_else(|| value.get("remainingQuantity"))
            .or_else(|| value.get("available_quantity"))
            .or_else(|| value.get("remaining_quantity"))
            .or_else(|| value.get("totalQuantity"))
            .or_else(|| value.get("quantity"))
            .and_then(as_decimal)
            .unwrap_or(Decimal::ZERO),
    })
}

fn parse_account_snapshot(frame: &str) -> Option<AccountSnapshot> {
    let value = serde_json::from_str::<Value>(frame).ok()?;
    let snapshot = value
        .get("result")
        .and_then(|result| result.get("accountSnapshot"))?;
    let positions = snapshot
        .get("positions")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(parse_position).collect())
        .unwrap_or_default();
    Some(AccountSnapshot { positions })
}

fn parse_side(value: &Value) -> Option<Side> {
    match value.as_str()? {
        "Bid" | "BID" | "BUY" | "Buy" | "bid" => Some(Side::Bid),
        "Ask" | "ASK" | "SELL" | "Sell" | "ask" => Some(Side::Ask),
        _ => None,
    }
}

fn parse_fill_side(value: &Value) -> Option<Side> {
    value
        .get("side")
        .or_else(|| value.get("takerSide"))
        .or_else(|| value.get("makerSide"))
        .or_else(|| value.get("direction"))
        .and_then(parse_side)
}

fn parse_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let value = value?;
    if let Some(ts) = value.as_i64() {
        return from_unix_like_timestamp(ts);
    }
    if let Some(ts) = value.as_str().and_then(|value| value.parse::<i64>().ok()) {
        return from_unix_like_timestamp(ts);
    }
    None
}

fn parse_event_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    parse_timestamp(value.get("timestamp"))
        .or_else(|| parse_timestamp(value.get("time")))
        .or_else(|| parse_timestamp(value.get("ts")))
}

fn extract_listen_key(frame: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(frame).ok()?;
    value
        .get("result")
        .and_then(|result| result.get("listenKey").or_else(|| result.get("listen_key")))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn market_ws_request_id(stream_name: &str) -> u64 {
    match stream_name {
        "mark_price" => 11,
        "spot_price" => 12,
        "ask_bid_price" => 13,
        "trades" => 14,
        "orderbook" => 15,
        _ => 99,
    }
}

fn from_unix_like_timestamp(ts: i64) -> Option<DateTime<Utc>> {
    if ts > 1_000_000_000_000 {
        DateTime::<Utc>::from_timestamp_millis(ts)
    } else {
        DateTime::<Utc>::from_timestamp(ts, 0)
    }
}

fn parse_book_side(value: &Value) -> Result<BTreeMap<Decimal, Decimal>> {
    let mut levels = BTreeMap::new();
    if value.is_null() {
        return Ok(levels);
    }
    if let Some(items) = value.get("levels").and_then(Value::as_array) {
        for item in items {
            levels.insert(
                parse_decimal(&item["price"])?,
                parse_decimal(&item["quantity"])?,
            );
        }
    }
    Ok(levels)
}

fn normalize_hex(secret: &str) -> Result<Vec<u8>> {
    let stripped = secret.strip_prefix("0x").unwrap_or(secret);
    Ok(hex::decode(stripped)?)
}

fn encode_recoverable_signature(signature: RecoverableSignature) -> String {
    let (recovery_id, compact) = signature.serialize_compact();
    let mut bytes = Vec::with_capacity(65);
    bytes.extend_from_slice(&compact);
    bytes.push(i32::from(recovery_id) as u8);
    hex::encode(bytes)
}

fn pow10_decimal(exp: u32) -> Decimal {
    let mut out = Decimal::ONE;
    for _ in 0..exp {
        out *= Decimal::from(10u64);
    }
    out
}

fn to_exchange_quantity(quantity: Decimal, decimals: u32) -> Result<u64> {
    let scaled = quantity * pow10_decimal(decimals);
    scaled
        .trunc()
        .to_u64()
        .ok_or_else(|| anyhow!("quantity overflow"))
}

fn to_exchange_price(
    price: Decimal,
    instrument: &InstrumentMeta,
    config: &VenueConfig,
) -> Result<u64> {
    let scale_diff = instrument.settlement_decimals as i32 - instrument.underlying_decimals as i32;
    let adjusted = if scale_diff >= 0 {
        price * pow10_decimal(scale_diff as u32)
    } else {
        price / pow10_decimal((-scale_diff) as u32)
    };
    let price_multiplier = Decimal::from_str(&config.price_multiplier)?;
    (adjusted * price_multiplier)
        .trunc()
        .to_u64()
        .ok_or_else(|| anyhow!("price overflow"))
}

fn to_rate_precision(value: Decimal, decimals: u32) -> Result<u64> {
    (value * pow10_decimal(decimals))
        .trunc()
        .to_u64()
        .ok_or_else(|| anyhow!("rate overflow"))
}

#[derive(Serialize)]
struct WsSubscription {
    symbol: String,
    topic: String,
}

impl WsSubscription {
    fn new(symbol: &str, topic: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            topic: topic.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeInfoResponse {
    future_contracts: Vec<ExchangeContract>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeContract {
    id: u32,
    symbol: String,
    underlying_decimals: u32,
    settlement_decimals: u32,
    min_order_size: Option<Decimal>,
    tick_size: Option<Decimal>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateOrdersRequest {
    account_id: u64,
    orders: Vec<CreateOrderPayload>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrdersBatchRequest {
    account_id: u64,
    orders: Vec<CancelOrderPayload>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateOrderPayload {
    action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    creation_deadline: Option<f64>,
    max_fees_percent: String,
    nonce: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_flags: Option<String>,
    order_type: OrderType,
    price: Option<Decimal>,
    quantity: Decimal,
    side: Side,
    signature: String,
    symbol: String,
    trigger_price: Option<Decimal>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrderPayload {
    action: String,
    order_id: String,
    signature: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelAllOrdersRequest {
    account_id: u64,
    nonce: u64,
    signature: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PendingOrder {
    order_id: Option<String>,
    nonce: Option<u64>,
    symbol: String,
    side: Side,
    price: Option<Decimal>,
    total_quantity: Decimal,
    available_quantity: Option<Decimal>,
}

#[derive(Default)]
struct AccountSnapshot {
    positions: Vec<Position>,
}

trait ValidateBatchResponse {
    fn validate(self, operation: &str) -> Result<()>;
}

fn parse_batch_results(response_text: &str) -> Result<Vec<BatchOrderResult>> {
    if let Ok(results) = serde_json::from_str::<Vec<BatchOrderResult>>(response_text) {
        return Ok(results);
    }

    let value: Value = serde_json::from_str(response_text)?;
    if let Some(items) = value
        .get("orders")
        .or_else(|| value.get("results"))
        .or_else(|| value.get("result"))
        .and_then(Value::as_array)
    {
        return items
            .iter()
            .cloned()
            .map(serde_json::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }

    bail!("unexpected batch response shape: {response_text}");
}

impl ValidateBatchResponse for Vec<BatchOrderResult> {
    fn validate(self, operation: &str) -> Result<()> {
        let rejected: Vec<BatchOrderResult> = self
            .into_iter()
            .filter(|result| !result.is_effective_success(operation))
            .collect();

        if rejected.is_empty() {
            return Ok(());
        }

        for result in &rejected {
            warn!(
                operation = operation,
                status = ?result.status,
                nonce = ?result.nonce,
                order_id = ?result.order_id,
                reason = ?result.reason,
                rejection_reason = ?result.rejection_reason,
                error = ?result.error,
                message = ?result.message,
                code = ?result.code,
                extra = ?result.extra,
                "hibachi batch order rejected"
            );
        }

        let reasons = rejected
            .iter()
            .map(|result| {
                format!(
                    "nonce={:?} order_id={:?} status={:?} reason={}",
                    result.nonce,
                    result.order_id,
                    result.status,
                    result.describe_reason()
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        bail!(
            "{} batch had {} rejected orders: {}",
            operation,
            rejected.len(),
            reasons
        );
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchOrderResult {
    status: Option<String>,
    nonce: Option<u64>,
    order_id: Option<String>,
    reason: Option<String>,
    rejection_reason: Option<String>,
    error: Option<String>,
    message: Option<String>,
    code: Option<String>,
    success: Option<bool>,
    accepted: Option<bool>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

impl BatchOrderResult {
    fn is_success(&self) -> bool {
        if let Some(success) = self.success {
            return success;
        }
        if let Some(accepted) = self.accepted {
            return accepted;
        }
        match self.status.as_deref() {
            Some("accepted") | Some("success") | Some("ok") | Some("placed")
            | Some("cancelled") => true,
            Some("rejected") | Some("error") | Some("failed") => false,
            _ => self.reason.is_none() && self.rejection_reason.is_none(),
        }
    }

    fn is_effective_success(&self, operation: &str) -> bool {
        if self.is_success() {
            return true;
        }

        if operation == "cancel" {
            let reason = self.describe_reason().to_ascii_lowercase();
            if reason.contains("already executed")
                || reason.contains("already cancelled")
                || reason.contains("already canceled")
                || reason.contains("order not found")
                || reason.contains("does not exist")
            {
                return true;
            }
        }

        false
    }

    fn describe_reason(&self) -> String {
        self.reason
            .clone()
            .or_else(|| self.rejection_reason.clone())
            .or_else(|| self.error.clone())
            .or_else(|| self.message.clone())
            .or_else(|| self.extra_reason())
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn extra_reason(&self) -> Option<String> {
        if self.extra.is_empty() {
            return None;
        }

        Some(
            self.extra
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}
