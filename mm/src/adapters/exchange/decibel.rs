use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, ORIGIN, USER_AGENT},
    Client,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock};
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
    config::{NetworkConfig, VenueConfig},
    domain::{
        Fill, FundingPaymentEvent, InstrumentMeta, MarketEvent, OpenOrder, OrderRequest, Position,
        PrivateEvent, Side,
    },
};

const SEC_WEBSOCKET_PROTOCOL: &str = "sec-websocket-protocol";

#[derive(Clone)]
pub struct DecibelClient {
    config: VenueConfig,
    http: Client,
    rest_governor: Arc<RestGovernor>,
    request_policy: RequestPolicy,
    market_by_symbol: Arc<RwLock<Option<HashMap<String, DecibelMarket>>>>,
    symbol_by_market_addr: Arc<RwLock<Option<HashMap<String, String>>>>,
}

impl DecibelClient {
    pub fn new(config: VenueConfig, network: &NetworkConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", config.api_key))
                .context("invalid Decibel bearer token")?,
        );
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&config.user_agent).context("invalid user-agent")?,
        );
        let origin = if config.decibel_origin.trim().is_empty() {
            "https://app.decibel.trade/trade"
        } else {
            config.decibel_origin.trim()
        };
        headers.insert(
            ORIGIN,
            HeaderValue::from_str(origin).context("invalid Decibel origin header")?,
        );

        let http = Client::builder().default_headers(headers).build()?;
        Ok(Self {
            config,
            http,
            rest_governor: Arc::new(RestGovernor::new(Duration::from_millis(
                network.private_rest_min_interval_ms,
            ))),
            request_policy: RequestPolicy::from_config(network),
            market_by_symbol: Arc::new(RwLock::new(None)),
            symbol_by_market_addr: Arc::new(RwLock::new(None)),
        })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.api_base_url.trim_end_matches('/'), path)
    }

    fn ws_url(&self) -> &str {
        if !self.config.ws_market_url.trim().is_empty() {
            self.config.ws_market_url.trim()
        } else {
            self.config.ws_account_url.trim()
        }
    }

    fn account_address(&self) -> Result<&str> {
        let account = self.config.decibel_account_address.trim();
        if account.is_empty() {
            bail!("decibel_account_address is required for Decibel integration");
        }
        Ok(account)
    }

    async fn load_markets_uncached(&self) -> Result<Vec<DecibelMarket>> {
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "decibel_load_markets", || {
                self.http.get(self.endpoint("/api/v1/markets")).send()
            })
            .await?;
        response.json().await.map_err(Into::into)
    }

    async fn ensure_market_cache(&self) -> Result<()> {
        if self.market_by_symbol.read().await.is_some() && self.symbol_by_market_addr.read().await.is_some() {
            return Ok(());
        }

        let markets = self.load_markets_uncached().await?;
        let mut by_symbol = HashMap::new();
        let mut by_addr = HashMap::new();
        for market in markets {
            by_addr.insert(market.market_addr.clone(), market.market_name.clone());
            by_symbol.insert(market.market_name.clone(), market);
        }
        *self.market_by_symbol.write().await = Some(by_symbol);
        *self.symbol_by_market_addr.write().await = Some(by_addr);
        Ok(())
    }

    async fn market_map(&self) -> Result<HashMap<String, DecibelMarket>> {
        self.ensure_market_cache().await?;
        Ok(self
            .market_by_symbol
            .read()
            .await
            .clone()
            .unwrap_or_default())
    }

    async fn symbol_for_market_addr(&self, market_addr: &str) -> Result<Option<String>> {
        self.ensure_market_cache().await?;
        Ok(self
            .symbol_by_market_addr
            .read()
            .await
            .as_ref()
            .and_then(|map| map.get(market_addr).cloned()))
    }

    async fn market_addrs_for_symbols(&self, symbols: &[String]) -> Result<Vec<(String, String)>> {
        let markets = self.market_map().await?;
        symbols
            .iter()
            .map(|symbol| {
                let market = markets
                    .get(symbol)
                    .with_context(|| format!("Decibel market not found for symbol {symbol}"))?;
                Ok((symbol.clone(), market.market_addr.clone()))
            })
            .collect()
    }

    fn websocket_protocol_header_value(&self) -> Result<HeaderValue> {
        HeaderValue::from_str(&format!("decibel, {}", self.config.api_key))
            .context("invalid Decibel websocket protocol header")
    }

    async fn connect_ws(&self) -> Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
        let mut request = self
            .ws_url()
            .into_client_request()
            .context("invalid Decibel websocket URL")?;
        request.headers_mut().insert(
            HeaderName::from_static(SEC_WEBSOCKET_PROTOCOL),
            self.websocket_protocol_header_value()?,
        );
        request.headers_mut().insert(
            USER_AGENT,
            HeaderValue::from_str(&self.config.user_agent)?,
        );
        let origin = if self.config.decibel_origin.trim().is_empty() {
            "https://app.decibel.trade/trade"
        } else {
            self.config.decibel_origin.trim()
        };
        request.headers_mut().insert(
            ORIGIN,
            HeaderValue::from_str(origin)?,
        );
        let (ws_stream, _) = connect_async(request).await?;
        Ok(ws_stream)
    }

    async fn subscribe_topics(
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        topics: &[String],
    ) -> Result<()> {
        for topic in topics {
            ws_stream
                .send(WsMessage::Text(
                    json!({
                        "method": "subscribe",
                        "topic": topic,
                    })
                    .to_string()
                    .into(),
                ))
                .await?;
        }
        Ok(())
    }

    async fn run_market_ws_loop(
        &self,
        stream_name: &'static str,
        topics: Vec<String>,
        sender: mpsc::Sender<MarketEvent>,
        parser: fn(&str, &HashMap<String, String>) -> Result<Vec<MarketEvent>>,
    ) -> Result<()> {
        self.ensure_market_cache().await?;
        let market_addr_to_symbol = self
            .symbol_by_market_addr
            .read()
            .await
            .clone()
            .unwrap_or_default();
        self.request_policy
            .run_ws_loop(
                stream_name,
                || {
                    let sender = sender.clone();
                    let topics = topics.clone();
                    let market_addr_to_symbol = market_addr_to_symbol.clone();
                    let client = self.clone();
                    async move {
                        let mut ws_stream = client.connect_ws().await?;
                        Self::subscribe_topics(&mut ws_stream, &topics).await?;
                        loop {
                            match ws_stream.next().await {
                                Some(Ok(WsMessage::Text(text))) => {
                                    let events = parser(&text, &market_addr_to_symbol)?;
                                    for event in events {
                                        sender.send(event).await?;
                                    }
                                }
                                Some(Ok(WsMessage::Ping(payload))) => {
                                    ws_stream.send(WsMessage::Pong(payload)).await?;
                                }
                                Some(Ok(WsMessage::Close(frame))) => {
                                    warn!(stream = stream_name, ?frame, "decibel websocket closed");
                                    break;
                                }
                                Some(Ok(_)) => {}
                                Some(Err(err)) => return Err(err.into()),
                                None => break,
                            }
                        }
                        bail!("decibel websocket closed")
                    }
                },
                || {
                    let sender = sender.clone();
                    let symbols = market_addr_to_symbol.values().cloned().collect::<Vec<_>>();
                    async move {
                        sender
                            .send(MarketEvent::StreamReconnected { symbols })
                            .await?;
                        Ok(())
                    }
                },
            )
            .await
    }

    async fn run_private_ws_once(&self, sender: mpsc::Sender<PrivateEvent>) -> Result<()> {
        self.send_private_snapshot(&sender).await?;
        let account = self.account_address()?.to_string();
        let topics = vec![
            format!("order_updates:{account}"),
            format!("account_positions:{account}"),
            format!("account_overview:{account}"),
            format!("user_trades:{account}"),
        ];
        let mut ws_stream = self.connect_ws().await?;
        Self::subscribe_topics(&mut ws_stream, &topics).await?;
        loop {
            match ws_stream.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    let events = self.parse_private_events(&text).await?;
                    for event in events {
                        sender.send(event).await?;
                    }
                }
                Some(Ok(WsMessage::Ping(payload))) => {
                    ws_stream.send(WsMessage::Pong(payload)).await?;
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    warn!(?frame, "decibel private websocket closed");
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(err.into()),
                None => break,
            }
        }
        bail!("decibel private websocket closed")
    }

    async fn send_private_snapshot(&self, sender: &mpsc::Sender<PrivateEvent>) -> Result<()> {
        let equity = self.fetch_account_equity().await?;
        sender.send(PrivateEvent::AccountEquity { equity }).await?;
        let open_orders = self.fetch_open_orders().await?;
        sender.send(PrivateEvent::OpenOrders(open_orders)).await?;
        let positions = self.fetch_positions().await?;
        for position in positions {
            sender.send(PrivateEvent::Position(position)).await?;
        }
        Ok(())
    }

    async fn fetch_account_equity(&self) -> Result<Decimal> {
        let account = self.account_address()?.to_string();
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "decibel_account_overview", || {
                self.http
                    .get(self.endpoint("/api/v1/account_overviews"))
                    .query(&[("account", account.as_str())])
                    .send()
            })
            .await?;
        let overview: DecibelAccountOverview = response.json().await?;
        Ok(decimal_from_f64(overview.perp_equity_balance))
    }

    async fn parse_private_events(&self, text: &str) -> Result<Vec<PrivateEvent>> {
        let value: Value = serde_json::from_str(text)?;
        if value.get("success").is_some() {
            return Ok(Vec::new());
        }
        let Some(topic) = value.get("topic").and_then(Value::as_str) else {
            return Ok(Vec::new());
        };

        if topic.starts_with("order_updates:") {
            let Some(order_value) = value
                .get("order")
                .and_then(|outer| outer.get("order"))
                .or_else(|| value.get("order"))
            else {
                return Ok(Vec::new());
            };
            let Some(order) = self.parse_open_order_value(order_value).await? else {
                return Ok(Vec::new());
            };
            let status = order_value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            if status == "open" && order.remaining_quantity > Decimal::ZERO {
                return Ok(vec![PrivateEvent::UpsertOpenOrder(order)]);
            }
            return Ok(vec![PrivateEvent::RemoveOpenOrder {
                order_id: order.order_id,
                nonce: None,
            }]);
        }

        if topic.starts_with("account_positions:") {
            let positions = value
                .get("positions")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut events = Vec::new();
            for position in positions {
                if let Some(position) = self.parse_position_value(&position).await? {
                    events.push(PrivateEvent::Position(position));
                }
            }
            return Ok(events);
        }

        if topic.starts_with("account_overview:") {
            if let Some(overview) = value.get("overview").or(Some(&value)) {
                if let Some(equity) = overview
                    .get("perp_equity_balance")
                    .and_then(value_to_decimal)
                {
                    return Ok(vec![PrivateEvent::AccountEquity { equity }]);
                }
            }
            return Ok(Vec::new());
        }

        if topic.starts_with("user_trades:") {
            let trades = value
                .get("trades")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut events = Vec::new();
            for trade in trades {
                if let Some(fill) = self.parse_fill_value(&trade).await? {
                    events.push(PrivateEvent::Fill(fill));
                }
            }
            return Ok(events);
        }

        Ok(Vec::new())
    }

    async fn parse_open_order_value(&self, value: &Value) -> Result<Option<OpenOrder>> {
        let Some(market_addr) = value.get("market").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(symbol) = self.symbol_for_market_addr(market_addr).await? else {
            return Ok(None);
        };
        let side = value
            .get("is_buy")
            .and_then(Value::as_bool)
            .map(|is_buy| if is_buy { Side::Bid } else { Side::Ask })
            .unwrap_or(Side::Bid);
        let remaining_quantity = value
            .get("remaining_size")
            .and_then(value_to_decimal)
            .unwrap_or(Decimal::ZERO);
        Ok(Some(OpenOrder {
            order_id: value
                .get("order_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            nonce: 0,
            level_index: None,
            symbol,
            side,
            price: value.get("price").and_then(value_to_decimal),
            remaining_quantity,
        }))
    }

    async fn parse_position_value(&self, value: &Value) -> Result<Option<Position>> {
        let Some(market_addr) = value.get("market").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(symbol) = self.symbol_for_market_addr(market_addr).await? else {
            return Ok(None);
        };
        let quantity = value
            .get("size")
            .and_then(value_to_decimal)
            .unwrap_or(Decimal::ZERO);
        Ok(Some(Position {
            symbol,
            quantity,
            entry_price: value
                .get("entry_price")
                .and_then(value_to_decimal)
                .unwrap_or(Decimal::ZERO),
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            pnl_is_authoritative: false,
            opened_at: None,
        }))
    }

    async fn parse_fill_value(&self, value: &Value) -> Result<Option<Fill>> {
        let Some(market_addr) = value.get("market").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(symbol) = self.symbol_for_market_addr(market_addr).await? else {
            return Ok(None);
        };
        let Some(price) = value.get("price").and_then(value_to_decimal) else {
            return Ok(None);
        };
        let Some(quantity) = value.get("size").and_then(value_to_decimal) else {
            return Ok(None);
        };
        let side = value
            .get("action")
            .and_then(Value::as_str)
            .and_then(side_from_action)
            .unwrap_or(Side::Bid);
        let timestamp = value
            .get("transaction_unix_ms")
            .and_then(Value::as_i64)
            .and_then(timestamp_millis_to_utc)
            .unwrap_or_else(Utc::now);
        Ok(Some(Fill {
            order_id: value
                .get("order_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            nonce: None,
            symbol,
            side,
            price,
            quantity,
            fee_paid: value.get("fee_amount").and_then(value_to_decimal),
            funding_paid: value
                .get("realized_funding_amount")
                .and_then(value_to_decimal),
            timestamp,
        }))
    }
}

#[async_trait]
impl MarketDataSource for DecibelClient {
    async fn stream_mark_prices(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let topics = self
            .market_addrs_for_symbols(symbols)
            .await?
            .into_iter()
            .map(|(_, market_addr)| format!("market_price:{market_addr}"))
            .collect();
        self.run_market_ws_loop("decibel_market_price", topics, sender, parse_decibel_market_price_events)
            .await
    }

    async fn stream_spot_prices(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let topics = self
            .market_addrs_for_symbols(symbols)
            .await?
            .into_iter()
            .map(|(_, market_addr)| format!("market_price:{market_addr}"))
            .collect();
        self.run_market_ws_loop("decibel_oracle_price", topics, sender, parse_decibel_oracle_price_events)
            .await
    }

    async fn stream_best_bid_ask(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let topics = self
            .market_addrs_for_symbols(symbols)
            .await?
            .into_iter()
            .map(|(_, market_addr)| format!("depth:{market_addr}:1"))
            .collect();
        self.run_market_ws_loop("decibel_depth_bbo", topics, sender, parse_decibel_bbo_events)
            .await
    }

    async fn stream_trades(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let topics = self
            .market_addrs_for_symbols(symbols)
            .await?
            .into_iter()
            .map(|(_, market_addr)| format!("trades:{market_addr}"))
            .collect();
        self.run_market_ws_loop("decibel_trades", topics, sender, parse_decibel_trade_events)
            .await
    }

    async fn stream_orderbook(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let topics = self
            .market_addrs_for_symbols(symbols)
            .await?
            .into_iter()
            .map(|(_, market_addr)| format!("depth:{market_addr}:1"))
            .collect();
        self.run_market_ws_loop("decibel_depth_book", topics, sender, parse_decibel_orderbook_events)
            .await
    }
}

#[async_trait]
impl PrivateDataSource for DecibelClient {
    async fn stream_private_data(
        &self,
        sender: mpsc::Sender<PrivateEvent>,
    ) -> Result<()> {
        self.request_policy
            .run_ws_loop(
                "decibel_private",
                || {
                    let sender = sender.clone();
                    let client = self.clone();
                    async move { client.run_private_ws_once(sender).await }
                },
                || {
                    let sender = sender.clone();
                    async move {
                        sender.send(PrivateEvent::StreamReconnected).await?;
                        Ok(())
                    }
                },
            )
            .await
    }

    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>> {
        let account = self.account_address()?.to_string();
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "decibel_open_orders", || {
                self.http
                    .get(self.endpoint("/api/v1/open_orders"))
                    .query(&[
                        ("user", account.as_str()),
                        ("pagination.limit", "200"),
                        ("pagination.offset", "0"),
                    ])
                    .send()
            })
            .await?;
        let response: DecibelPaginated<DecibelOpenOrderItem> = response.json().await?;
        let mut orders = Vec::new();
        for item in response.items {
            if let Some(order) = self.parse_open_order_value(&serde_json::to_value(item)?).await? {
                orders.push(order);
            }
        }
        Ok(orders)
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>> {
        let account = self.account_address()?.to_string();
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "decibel_positions", || {
                self.http
                    .get(self.endpoint("/api/v1/account_positions"))
                    .query(&[
                        ("account", account.as_str()),
                        ("limit", "500"),
                        ("include_deleted", "false"),
                    ])
                    .send()
            })
            .await?;
        let response: Vec<DecibelPositionItem> = response.json().await?;
        let mut positions = Vec::new();
        for item in response {
            if item.is_deleted {
                continue;
            }
            if let Some(position) = self.parse_position_value(&serde_json::to_value(item)?).await? {
                positions.push(position);
            }
        }
        Ok(positions)
    }
}

#[async_trait]
impl OrderExecutor for DecibelClient {
    async fn place_orders(&self, _requests: Vec<OrderRequest>) -> Result<()> {
        bail!(
            "Decibel Rust adapter is currently read-only. Order placement requires the on-chain write path / transaction builder."
        )
    }

    async fn cancel_orders(&self, _order_ids: Vec<String>) -> Result<()> {
        bail!(
            "Decibel Rust adapter is currently read-only. Order cancellation requires the on-chain write path / transaction builder."
        )
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        bail!(
            "Decibel Rust adapter is currently read-only. Cancel-all requires the on-chain write path / transaction builder."
        )
    }
}

#[async_trait]
impl ExchangeClient for DecibelClient {
    async fn load_instruments(&self) -> Result<Vec<InstrumentMeta>> {
        let markets = self.market_map().await?;
        markets
            .into_values()
            .map(|market| {
                Ok(InstrumentMeta {
                    symbol: market.market_name,
                    contract_id: 0,
                    underlying_decimals: market.sz_decimals as u32,
                    settlement_decimals: market.px_decimals as u32,
                    min_order_size: decimal_from_i64(market.min_size),
                    tick_size: Some(decimal_from_i64(market.tick_size)),
                })
            })
            .collect()
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Vec<MarketEvent> {
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "decibel_prices", || {
                self.http.get(self.endpoint("/api/v1/prices")).send()
            })
            .await;
        let Ok(response) = response else {
            return Vec::new();
        };
        let Ok(prices) = response.json::<Vec<DecibelPriceItem>>().await else {
            return Vec::new();
        };
        let Ok(symbol_by_market_addr) = self.symbol_by_market_addr.read().await.clone().ok_or(()) else {
            return Vec::new();
        };
        let wanted: std::collections::HashSet<&str> = symbols.iter().map(String::as_str).collect();
        let mut events = Vec::new();
        for price in prices {
            let Some(symbol) = symbol_by_market_addr.get(&price.market) else {
                continue;
            };
            if !wanted.contains(symbol.as_str()) {
                continue;
            }
            let timestamp =
                timestamp_millis_to_utc(price.transaction_unix_ms).unwrap_or_else(Utc::now);
            events.push(MarketEvent::MarkPrice {
                symbol: symbol.clone(),
                price: decimal_from_f64(price.mark_px),
                timestamp,
            });
            events.push(MarketEvent::SpotPrice {
                symbol: symbol.clone(),
                price: decimal_from_f64(price.oracle_px),
                timestamp,
            });
            let funding_rate_bps = decimal_from_f64(price.funding_rate_bps);
            let signed_rate_bps = if price.is_funding_positive {
                funding_rate_bps
            } else {
                -funding_rate_bps
            };
            events.push(MarketEvent::FundingRate {
                symbol: symbol.clone(),
                rate: signed_rate_bps / Decimal::from(10_000u64),
                timestamp,
            });
        }
        events
    }

    async fn fetch_funding_payments(
        &self,
        symbols: &[String],
        start_time: DateTime<Utc>,
    ) -> Result<Vec<FundingPaymentEvent>> {
        let account = self.account_address()?.to_string();
        let market_map = self.market_map().await?;
        let symbol_to_addr: HashMap<String, String> = market_map
            .values()
            .map(|market| (market.market_name.clone(), market.market_addr.clone()))
            .collect();
        let wanted_addrs: std::collections::HashSet<String> = symbols
            .iter()
            .filter_map(|symbol| symbol_to_addr.get(symbol).cloned())
            .collect();
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "decibel_funding_history", || {
                self.http
                    .get(self.endpoint("/api/v1/funding_rate_history"))
                    .query(&[
                        ("account", account.as_str()),
                        ("pagination.limit", "200"),
                        ("pagination.offset", "0"),
                    ])
                    .send()
            })
            .await?;
        let response: DecibelPaginated<DecibelFundingHistoryItem> = response.json().await?;
        let mut events = Vec::new();
        for item in response.items {
            if item.transaction_unix_ms < start_time.timestamp_millis() {
                continue;
            }
            if !wanted_addrs.is_empty() && !wanted_addrs.contains(&item.market) {
                continue;
            }
            let Some(symbol) = self.symbol_for_market_addr(&item.market).await? else {
                continue;
            };
            let ts = timestamp_millis_to_utc(item.transaction_unix_ms).unwrap_or_else(Utc::now);
            let amount = decimal_from_f64(item.realized_funding_amount);
            let fee_amount = decimal_from_f64(item.fee_amount);
            events.push(FundingPaymentEvent {
                ts,
                symbol: Some(symbol),
                amount,
                payment_type: "funding_payment".to_string(),
                note: Some(format!(
                    "venue=decibel action={} fee_amount={} is_rebate={} size={}",
                    item.action, fee_amount, item.is_rebate, item.size
                )),
                is_simulated: false,
            });
        }
        Ok(events)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct DecibelMarket {
    lot_size: i64,
    market_addr: String,
    market_name: String,
    max_leverage: i32,
    max_open_interest: f64,
    min_size: i64,
    mode: String,
    px_decimals: i32,
    sz_decimals: i32,
    tick_size: i64,
    unrealized_pnl_haircut_bps: i32,
}

#[derive(Clone, Debug, Deserialize)]
struct DecibelPriceItem {
    market: String,
    oracle_px: f64,
    mark_px: f64,
    mid_px: f64,
    funding_rate_bps: f64,
    is_funding_positive: bool,
    transaction_unix_ms: i64,
    open_interest: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct DecibelPaginated<T> {
    items: Vec<T>,
    total_count: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DecibelOpenOrderItem {
    client_order_id: Option<String>,
    details: Option<String>,
    is_buy: bool,
    is_reduce_only: bool,
    market: String,
    order_direction: Option<String>,
    order_id: String,
    order_type: Option<String>,
    parent: Option<String>,
    status: String,
    transaction_version: i64,
    trigger_condition: Option<String>,
    unix_ms: i64,
    orig_size: f64,
    price: f64,
    remaining_size: f64,
    size_delta: Option<f64>,
    sl_limit_price: Option<f64>,
    sl_order_id: Option<String>,
    sl_trigger_price: Option<f64>,
    tp_limit_price: Option<f64>,
    tp_order_id: Option<String>,
    tp_trigger_price: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DecibelPositionItem {
    entry_price: f64,
    estimated_liquidation_price: f64,
    has_fixed_sized_tpsls: bool,
    is_deleted: bool,
    is_isolated: bool,
    market: String,
    size: f64,
    transaction_version: i64,
    unrealized_funding: f64,
    user: String,
    user_leverage: i32,
    sl_limit_price: Option<f64>,
    sl_order_id: Option<String>,
    sl_trigger_price: Option<f64>,
    tp_limit_price: Option<f64>,
    tp_order_id: Option<String>,
    tp_trigger_price: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
struct DecibelAccountOverview {
    perp_equity_balance: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct DecibelFundingHistoryItem {
    action: String,
    fee_amount: f64,
    is_rebate: bool,
    market: String,
    realized_funding_amount: f64,
    size: f64,
    transaction_unix_ms: i64,
}

fn parse_decibel_market_price_events(
    text: &str,
    market_addr_to_symbol: &HashMap<String, String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("success").is_some() {
        return Ok(Vec::new());
    }
    let Some(price) = value.get("price") else {
        return Ok(Vec::new());
    };
    let Some(market_addr) = price.get("market").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    let Some(symbol) = market_addr_to_symbol.get(market_addr) else {
        return Ok(Vec::new());
    };
    let timestamp = price
        .get("transaction_unix_ms")
        .and_then(Value::as_i64)
        .and_then(timestamp_millis_to_utc)
        .unwrap_or_else(Utc::now);
    let mark_px = price.get("mark_px").and_then(value_to_decimal);
    let funding_rate_bps = price
        .get("funding_rate_bps")
        .and_then(value_to_decimal)
        .unwrap_or(Decimal::ZERO);
    let signed_rate_bps = if price
        .get("is_funding_positive")
        .and_then(Value::as_bool)
        .unwrap_or(true)
    {
        funding_rate_bps
    } else {
        -funding_rate_bps
    };

    let mut events = Vec::new();
    if let Some(mark_px) = mark_px {
        events.push(MarketEvent::MarkPrice {
            symbol: symbol.clone(),
            price: mark_px,
            timestamp,
        });
    }
    events.push(MarketEvent::FundingRate {
        symbol: symbol.clone(),
        rate: signed_rate_bps / Decimal::from(10_000u64),
        timestamp,
    });
    Ok(events)
}

fn parse_decibel_oracle_price_events(
    text: &str,
    market_addr_to_symbol: &HashMap<String, String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("success").is_some() {
        return Ok(Vec::new());
    }
    let Some(price) = value.get("price") else {
        return Ok(Vec::new());
    };
    let Some(market_addr) = price.get("market").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    let Some(symbol) = market_addr_to_symbol.get(market_addr) else {
        return Ok(Vec::new());
    };
    let Some(oracle_px) = price.get("oracle_px").and_then(value_to_decimal) else {
        return Ok(Vec::new());
    };
    let timestamp = price
        .get("transaction_unix_ms")
        .and_then(Value::as_i64)
        .and_then(timestamp_millis_to_utc)
        .unwrap_or_else(Utc::now);
    Ok(vec![MarketEvent::SpotPrice {
        symbol: symbol.clone(),
        price: oracle_px,
        timestamp,
    }])
}

fn parse_decibel_bbo_events(
    text: &str,
    market_addr_to_symbol: &HashMap<String, String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("success").is_some() {
        return Ok(Vec::new());
    }
    let Some(market_addr) = value.get("market").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    let Some(symbol) = market_addr_to_symbol.get(market_addr) else {
        return Ok(Vec::new());
    };
    let bids = value
        .get("bids")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let asks = value
        .get("asks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let Some(best_bid) = bids.first().and_then(level_price_and_size) else {
        return Ok(Vec::new());
    };
    let Some(best_ask) = asks.first().and_then(level_price_and_size) else {
        return Ok(Vec::new());
    };
    Ok(vec![MarketEvent::BestBidAsk {
        symbol: symbol.clone(),
        bid: best_bid.0,
        ask: best_ask.0,
        bid_size: Some(best_bid.1),
        ask_size: Some(best_ask.1),
        timestamp: Utc::now(),
    }])
}

fn parse_decibel_orderbook_events(
    text: &str,
    market_addr_to_symbol: &HashMap<String, String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("success").is_some() {
        return Ok(Vec::new());
    }
    let Some(market_addr) = value.get("market").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    let Some(symbol) = market_addr_to_symbol.get(market_addr) else {
        return Ok(Vec::new());
    };
    let bids = value
        .get("bids")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let asks = value
        .get("asks")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut bid_map = BTreeMap::new();
    let mut ask_map = BTreeMap::new();
    for level in bids {
        if let Some((price, size)) = level_price_and_size(&level) {
            bid_map.insert(price, size);
        }
    }
    for level in asks {
        if let Some((price, size)) = level_price_and_size(&level) {
            ask_map.insert(price, size);
        }
    }
    if bid_map.is_empty() && ask_map.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![MarketEvent::OrderBookSnapshot {
        symbol: symbol.clone(),
        bids: bid_map,
        asks: ask_map,
        timestamp: Utc::now(),
    }])
}

fn parse_decibel_trade_events(
    text: &str,
    market_addr_to_symbol: &HashMap<String, String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("success").is_some() {
        return Ok(Vec::new());
    }
    let Some(market_addr) = value.get("market").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    let Some(symbol) = market_addr_to_symbol.get(market_addr) else {
        return Ok(Vec::new());
    };
    let trades = value
        .get("trades")
        .or_else(|| value.get("items"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut events = Vec::new();
    for trade in trades {
        let Some(price) = trade.get("price").and_then(value_to_decimal) else {
            continue;
        };
        let Some(quantity) = trade
            .get("size")
            .or_else(|| trade.get("qty"))
            .and_then(value_to_decimal)
        else {
            continue;
        };
        let taker_side = trade
            .get("side")
            .and_then(Value::as_str)
            .and_then(side_from_trade_side);
        let timestamp = trade
            .get("transaction_unix_ms")
            .or_else(|| trade.get("unix_ms"))
            .and_then(Value::as_i64)
            .and_then(timestamp_millis_to_utc)
            .unwrap_or_else(Utc::now);
        events.push(MarketEvent::Trade {
            symbol: symbol.clone(),
            price,
            quantity,
            taker_side,
            timestamp,
        });
    }
    Ok(events)
}

fn level_price_and_size(level: &Value) -> Option<(Decimal, Decimal)> {
    Some((
        level.get("price").and_then(value_to_decimal)?,
        level.get("size").and_then(value_to_decimal)?,
    ))
}

fn value_to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => Decimal::from_str(text).ok(),
        Value::Number(number) => Decimal::from_str(&number.to_string()).ok(),
        _ => None,
    }
}

fn decimal_from_f64(value: f64) -> Decimal {
    Decimal::from_str(&value.to_string()).unwrap_or(Decimal::ZERO)
}

fn decimal_from_i64(value: i64) -> Decimal {
    Decimal::from_i128_with_scale(value as i128, 0)
}

fn timestamp_millis_to_utc(timestamp_ms: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(timestamp_ms).single()
}

fn side_from_trade_side(side: &str) -> Option<Side> {
    let normalized = side.trim().to_ascii_lowercase();
    if normalized.contains("buy") || normalized.contains("bid") {
        Some(Side::Bid)
    } else if normalized.contains("sell") || normalized.contains("ask") {
        Some(Side::Ask)
    } else {
        None
    }
}

fn side_from_action(action: &str) -> Option<Side> {
    let normalized = action.trim().to_ascii_lowercase();
    if normalized.contains("open long") || normalized.contains("close short") {
        Some(Side::Bid)
    } else if normalized.contains("open short") || normalized.contains("close long") {
        Some(Side::Ask)
    } else {
        None
    }
}
