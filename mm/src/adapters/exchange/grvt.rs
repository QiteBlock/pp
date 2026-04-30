use std::{
    collections::{BTreeMap, HashMap},
    future::pending,
    str::FromStr,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ethers_core::{
    abi::{encode, Token},
    types::{Address, Signature as EvmSignature, H256, U256},
    utils::keccak256,
};
use ethers_signers::{LocalWallet, Signer};
use futures_util::{SinkExt, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderValue, COOKIE, SET_COOKIE, USER_AGENT},
    Client,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message as WsMessage},
};
use tracing::{error, info, warn};

use crate::{
    adapters::{
        exchange::{ExchangeClient, MarketDataSource, OrderExecutor, PrivateDataSource},
        network::{RequestPolicy, RestGovernor},
    },
    config::{NetworkConfig, VenueConfig},
    domain::{
        InstrumentMeta, MarketEvent, OpenOrder, OrderRequest, OrderType, Position, PrivateEvent,
        Side,
    },
};

const GRVT_PRICE_MULTIPLIER: u64 = 1_000_000_000;

#[derive(Clone)]
pub struct GrvtClient {
    config: VenueConfig,
    http: Client,
    rest_governor: Arc<RestGovernor>,
    order_governor: Arc<RestGovernor>,
    request_policy: RequestPolicy,
    session_cookie: Arc<RwLock<Option<String>>>,
    session_account_id: Arc<RwLock<Option<String>>>,
    instrument_cache: Arc<RwLock<Option<HashMap<String, GrvtInstrument>>>>,
    /// Monotonic counter for unique order nonces; avoids collisions with parallel placements.
    nonce_counter: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
struct GrvtInstrument {
    meta: InstrumentMeta,
    instrument_hash: String,
    base_decimals: i32,
    min_size: Decimal,
}

impl GrvtClient {
    pub fn new(config: VenueConfig, network: &NetworkConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&config.user_agent).context("invalid user-agent")?,
        );
        let http = Client::builder().default_headers(headers).build()?;
        Ok(Self {
            config,
            http,
            rest_governor: Arc::new(RestGovernor::new(Duration::from_millis(
                network.private_rest_min_interval_ms,
            ))),
            // Separate low-latency governor for order placement/cancellation.
            // 15ms allows up to ~66 order ops/sec while still preventing runaway loops.
            order_governor: Arc::new(RestGovernor::new(Duration::from_millis(15))),
            request_policy: RequestPolicy::from_config(network),
            session_cookie: Arc::new(RwLock::new(None)),
            session_account_id: Arc::new(RwLock::new(None)),
            instrument_cache: Arc::new(RwLock::new(None)),
            nonce_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    fn auth_endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.grvt_auth_base_url, path)
    }

    fn market_data_endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.grvt_market_data_base_url, path)
    }

    fn trading_endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.grvt_trading_base_url, path)
    }

    async fn ensure_session(&self) -> Result<(String, String)> {
        if let (Some(cookie), Some(account_id)) = (
            self.session_cookie.read().await.clone(),
            self.session_account_id.read().await.clone(),
        ) {
            return Ok((cookie, account_id));
        }

        let (login_url, payload) = if !self.config.grvt_api_key.trim().is_empty() {
            info!("grvt api-key login starting");
            (
                self.auth_endpoint("/auth/api_key/login"),
                json!({
                    "api_key": self.config.grvt_api_key
                }),
            )
        } else {
            info!("grvt wallet login starting");
            let wallet: LocalWallet = self
                .config
                .grvt_private_key
                .parse()
                .context("invalid grvt private key")?;
            let signer = format!("{:#x}", wallet.address());
            let nonce = self.now_grvt_nonce();
            let expiration = self.server_time_nanos().await? + 300_000_000_000i64;
            info!(signer = %signer, nonce, expiration, chain_id = %self.config.grvt_chain_id, "grvt wallet login payload prepared");
            info!("grvt wallet login signing typed data");
            let signature: EvmSignature = match sign_grvt_wallet_login(
                &wallet,
                &signer,
                nonce,
                expiration,
                self.grvt_chain_id(),
            ) {
                Ok(signature) => signature,
                Err(err) => {
                    error!(?err, "grvt wallet login signing failed");
                    return Err(err).context("grvt wallet login signing failed");
                }
            };
            info!(
                v = signature.v as u64,
                "grvt wallet login signature created"
            );
            (
                self.auth_endpoint("/auth/wallet/login"),
                json!({
                    "address": signer,
                    "signature": {
                        "signer": format!("{:#x}", wallet.address()),
                        "r": format!("{:#066x}", signature.r),
                        "s": format!("{:#066x}", signature.s),
                        "v": signature.v as u64,
                        "expiration": expiration.to_string(),
                        "nonce": nonce,
                        "chain_id": self.config.grvt_chain_id
                    }
                }),
            )
        };
        info!(url = %login_url, "grvt auth request sending");
        self.rest_governor.until_ready().await;
        let response = self
            .http
            .post(&login_url)
            .timeout(Duration::from_secs(15))
            .header(COOKIE, "rm=true;")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("grvt auth request failed for {login_url}"))?;

        let status = response.status();
        let cookie = extract_grvt_cookie(response.headers());
        let account_id = extract_grvt_account_id(response.headers());
        let body = response.text().await.unwrap_or_default();
        info!(%status, body = %body, "grvt auth response received");
        if !status.is_success() {
            error!(%status, body = %body, "grvt auth failed");
            bail!("grvt auth failed with HTTP {status}: {body}");
        }

        let cookie = cookie.context(format!(
            "grvt login did not return a gravity session cookie; response body: {body}"
        ))?;
        let account_id = account_id.unwrap_or_else(|| self.config.grvt_account_id.clone());
        *self.session_cookie.write().await = Some(cookie.clone());
        *self.session_account_id.write().await = Some(account_id.clone());
        info!(account_id = %account_id, "grvt auth succeeded");
        Ok((cookie, account_id))
    }

    async fn server_time_nanos(&self) -> Result<i64> {
        let response: GrvtServerTimeResponse = self
            .request_policy
            .send_with_retry(&self.rest_governor, "grvt_server_time", || {
                self.http.get(self.market_data_endpoint("/time")).send()
            })
            .await?
            .json()
            .await?;
        let nanos = response
            .server_time
            .checked_mul(1_000_000)
            .context("grvt server time nanoseconds overflow")?;
        let nanos = i64::try_from(nanos).context("grvt server time does not fit i64")?;
        info!(
            server_time_ms = response.server_time,
            server_time_ns = nanos,
            "grvt server time fetched"
        );
        Ok(nanos)
    }

    /// Like `post_trading` but uses the dedicated low-latency order governor (30ms)
    /// instead of the general private REST governor, so parallel order placements
    /// don't serialize behind the general 200ms rate limit.
    async fn post_trading_order<T: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<Value> {
        let (cookie, account_id) = self.ensure_session().await?;
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_str(&cookie)?);
        headers.insert("X-Grvt-Account-Id", HeaderValue::from_str(&account_id)?);

        let response = self
            .request_policy
            .send_with_retry_allow_status(&self.order_governor, path, || {
                self.http
                    .post(self.trading_endpoint(path))
                    .headers(headers.clone())
                    .json(body)
                    .send()
            })
            .await?;
        let status = response.status();
        let body_text = response.text().await?;
        if !status.is_success() {
            bail!("grvt {path} failed with HTTP {status}: {body_text}");
        }
        serde_json::from_str(&body_text).map_err(Into::into)
    }

    async fn post_trading<T: Serialize + ?Sized>(&self, path: &str, body: &T) -> Result<Value> {
        let (cookie, account_id) = self.ensure_session().await?;
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_str(&cookie)?);
        headers.insert("X-Grvt-Account-Id", HeaderValue::from_str(&account_id)?);

        let response = self
            .request_policy
            .send_with_retry_allow_status(&self.rest_governor, path, || {
                self.http
                    .post(self.trading_endpoint(path))
                    .headers(headers.clone())
                    .json(body)
                    .send()
            })
            .await?;
        let status = response.status();
        let body_text = response.text().await?;
        if !status.is_success() {
            bail!("grvt {path} failed with HTTP {status}: {body_text}");
        }
        serde_json::from_str(&body_text).map_err(Into::into)
    }

    pub async fn fetch_best_bid_ask_snapshot(&self, symbol: &str) -> Result<(Decimal, Decimal)> {
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "grvt_ticker", || {
                self.http
                    .post(self.market_data_endpoint("/full/v1/ticker"))
                    .json(&json!({
                        "instrument": grvt_symbol_from_internal(symbol)
                    }))
                    .send()
            })
            .await?;
        let payload: Value = response.json().await?;
        let result = payload.get("result").unwrap_or(&payload);
        let bid = result
            .get("best_bid_price")
            .or_else(|| result.get("bb"))
            .and_then(as_decimal)
            .with_context(|| format!("missing GRVT best bid for {symbol}"))?;
        let ask = result
            .get("best_ask_price")
            .or_else(|| result.get("ba"))
            .and_then(as_decimal)
            .with_context(|| format!("missing GRVT best ask for {symbol}"))?;
        Ok((bid, ask))
    }

    pub async fn submit_limit_close_orders(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<()> {
        let instruments = self.instruments_by_symbol().await?;
        let mut orders = Vec::new();
        for position in positions
            .iter()
            .filter(|position| position.quantity != Decimal::ZERO)
        {
            let (bid, ask) = self.fetch_best_bid_ask_snapshot(&position.symbol).await?;
            let instrument = instruments
                .get(&position.symbol)
                .with_context(|| format!("missing grvt instrument for {}", position.symbol))?;
            // Place on the passive (maker) side, but improve by one tick toward mid when
            // the spread allows it. Joining the stale best quote can leave small startup
            // residuals sitting at the back of queue for minutes.
            let (side, price) = if position.quantity.is_sign_positive() {
                (
                    Side::Ask,
                    cleanup_limit_price(Side::Ask, bid, ask, instrument.meta.tick_size),
                )
            } else {
                (
                    Side::Bid,
                    cleanup_limit_price(Side::Bid, bid, ask, instrument.meta.tick_size),
                )
            };
            let notional = position.quantity.abs() * price;
            if notional < min_close_notional {
                warn!(
                    symbol = %position.symbol,
                    quantity = %position.quantity,
                    price = %price,
                    notional = %notional,
                    min_close_notional = %min_close_notional,
                    "skipping residual position in cleanup below configured minimum notional"
                );
                continue;
            }
            orders.push(OrderRequest {
                symbol: position.symbol.clone(),
                contract_id: 0,
                level_index: 0,
                side,
                order_type: OrderType::Limit,
                price: Some(price),
                quantity: position.quantity.abs(),
                post_only: true,
            });
        }
        if orders.is_empty() {
            return Ok(());
        }
        self.place_orders(orders).await
    }

    pub async fn active_cleanup_positions(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<Vec<Position>> {
        let mut active = Vec::new();
        for position in positions
            .iter()
            .filter(|position| position.quantity != Decimal::ZERO)
        {
            let (bid, ask) = self.fetch_best_bid_ask_snapshot(&position.symbol).await?;
            let reference_price = if position.quantity.is_sign_positive() {
                bid.max(Decimal::ZERO)
            } else {
                ask.max(Decimal::ZERO)
            };
            if reference_price <= Decimal::ZERO {
                active.push(position.clone());
                continue;
            }
            let notional = position.quantity.abs() * reference_price;
            if notional >= min_close_notional {
                active.push(position.clone());
            } else {
                info!(
                    symbol = %position.symbol,
                    quantity = %position.quantity,
                    reference_price = %reference_price,
                    notional = %notional,
                    min_close_notional = %min_close_notional,
                    "treating grvt cleanup position as dust below live minimum notional"
                );
            }
        }
        Ok(active)
    }

    pub async fn submit_market_close_orders(
        &self,
        positions: &[Position],
        min_close_notional: Decimal,
    ) -> Result<()> {
        let mut orders = Vec::new();
        for position in positions.iter().filter(|p| p.quantity != Decimal::ZERO) {
            // Use a rough mark price estimate for the dust-notional check.
            // For market orders the price field is unused by the exchange.
            let (bid, ask) = self.fetch_best_bid_ask_snapshot(&position.symbol).await?;
            let ref_price = if position.quantity.is_sign_positive() {
                ask
            } else {
                bid
            };
            let notional = position.quantity.abs() * ref_price;
            if notional < min_close_notional {
                warn!(
                    symbol = %position.symbol,
                    quantity = %position.quantity,
                    notional = %notional,
                    min_close_notional = %min_close_notional,
                    "skipping residual position in market cleanup below configured minimum notional"
                );
                continue;
            }
            let side = if position.quantity.is_sign_positive() {
                Side::Ask
            } else {
                Side::Bid
            };
            orders.push(OrderRequest {
                symbol: position.symbol.clone(),
                contract_id: 0,
                level_index: 0,
                side,
                order_type: OrderType::Market,
                price: None,
                quantity: position.quantity.abs(),
                post_only: false,
            });
        }
        if orders.is_empty() {
            return Ok(());
        }
        self.place_orders(orders).await
    }

    async fn load_instruments_uncached(&self) -> Result<Vec<GrvtInstrument>> {
        info!("grvt loading instruments");
        let response: GrvtAllInstrumentsResponse = self
            .request_policy
            .send_with_retry(&self.rest_governor, "grvt_all_instruments", || {
                self.http
                    .post(self.market_data_endpoint("/full/v1/all_instruments"))
                    .json(&json!({
                        "kind": ["PERPETUAL"],
                        "is_active": true,
                        "limit": 1000
                    }))
                    .send()
            })
            .await?
            .json()
            .await?;
        info!(
            count = response.result.len(),
            "grvt instruments response received"
        );

        response
            .result
            .into_iter()
            .filter(|instrument| {
                instrument.kind == "PERPETUAL"
                    && instrument.venues.iter().any(|venue| venue == "ORDERBOOK")
            })
            .map(|instrument| {
                let symbol = grvt_symbol_to_internal(&instrument.instrument);
                Ok(GrvtInstrument {
                    instrument_hash: instrument.instrument_hash,
                    base_decimals: instrument.base_decimals,
                    min_size: instrument.min_size,
                    meta: InstrumentMeta {
                        symbol,
                        contract_id: 0,
                        underlying_decimals: instrument.base_decimals.max(0) as u32,
                        settlement_decimals: 9,
                        min_order_size: instrument.min_size,
                        tick_size: Some(instrument.tick_size),
                    },
                })
            })
            .collect()
    }

    async fn instruments_by_symbol(&self) -> Result<HashMap<String, GrvtInstrument>> {
        if let Some(cache) = self.instrument_cache.read().await.as_ref() {
            return Ok(cache.clone());
        }

        let instruments = self.load_instruments_uncached().await?;
        let by_symbol = instruments
            .into_iter()
            .map(|instrument| (instrument.meta.symbol.clone(), instrument))
            .collect::<HashMap<_, _>>();
        *self.instrument_cache.write().await = Some(by_symbol.clone());
        info!(count = by_symbol.len(), "grvt instrument cache seeded");
        Ok(by_symbol)
    }

    async fn stream_market_topic(
        &self,
        stream_name: &'static str,
        selectors: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.request_policy
            .run_ws_loop(
                stream_name,
                || {
                    let url = self.config.grvt_market_ws_url.clone();
                    let selectors = selectors.to_vec();
                    let sender = sender.clone();
                    async move {
                        let raw_frame_count = AtomicUsize::new(0);
                        let parsed_frame_count = AtomicUsize::new(0);
                        info!(stream = stream_name, selectors = ?selectors, "grvt market websocket connecting");
                        let (mut ws_stream, _) = connect_async(&url).await?;
                        let subscribe = serde_json::to_string(&json!({
                            "jsonrpc": "2.0",
                            "method": "subscribe",
                            "params": {
                                "stream": stream_name,
                                "selectors": selectors,
                            },
                            "id": grvt_request_id(stream_name),
                        }))?;
                        ws_stream.send(WsMessage::Text(subscribe.into())).await?;
                        info!(stream = stream_name, "grvt market websocket subscribed");

                        let mut keepalive = {
                            let mut t = tokio::time::interval(Duration::from_secs(10));
                            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            t.tick().await;
                            t
                        };
                        loop {
                            tokio::select! {
                                _ = keepalive.tick() => {
                                    ws_stream.send(WsMessage::Ping(vec![].into())).await?;
                                }
                                message = ws_stream.next() => {
                                    let Some(msg) = message else { break };
                                    match msg? {
                                        WsMessage::Text(text) => {
                                            raw_frame_count.fetch_add(1, Ordering::Relaxed);
                                            if let Some(error_message) = parse_grvt_ws_error(&text) {
                                                warn!(
                                                    stream = stream_name,
                                                    error = %error_message,
                                                    frame = %text,
                                                    "grvt market websocket returned error"
                                                );
                                                continue;
                                            }
                                            let events = parse_market_events(stream_name, &text)?;
                                            if !events.is_empty() {
                                                parsed_frame_count.fetch_add(1, Ordering::Relaxed);
                                            }
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
                                                "grvt market websocket closed"
                                            );
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }

                        bail!("grvt market websocket closed")
                    }
                },
                || {
                    let symbols = selectors
                        .iter()
                        .map(|selector| selector.split('@').next().unwrap_or(selector))
                        .map(grvt_symbol_to_internal)
                        .collect::<Vec<_>>();
                    let sender = sender.clone();
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

    async fn build_grvt_order_payload(
        &self,
        request: &OrderRequest,
        instrument: &GrvtInstrument,
    ) -> Result<Value> {
        let wallet: LocalWallet = self
            .config
            .grvt_private_key
            .parse()
            .context("invalid grvt private key")?;
        let nonce = self.now_grvt_nonce();
        let expiration = self.now_nanos()? + (self.config.creation_deadline_ms as i64 * 1_000_000);
        let quantized_size = grvt_quantize_size(
            request.quantity,
            instrument.base_decimals,
            instrument.min_size,
        );
        let limit_price = if request.order_type == OrderType::Market {
            Decimal::ZERO
        } else {
            grvt_quantize_price(
                request.price.unwrap_or(Decimal::ZERO),
                instrument.meta.tick_size,
            )
        };
        let normalized_request = OrderRequest {
            quantity: quantized_size,
            price: if request.order_type == OrderType::Market {
                None
            } else {
                Some(limit_price)
            },
            ..request.clone()
        };

        let signature = sign_grvt_order(
            &wallet,
            &self.config.grvt_sub_account_id,
            &normalized_request,
            instrument,
            nonce,
            expiration,
            self.config.grvt_chain_id.parse::<u64>().unwrap_or(325),
        )?;
        let signer = format!("{:#x}", wallet.address());
        let mut order = json!({
            "sub_account_id": self.config.grvt_sub_account_id,
            "is_market": request.order_type == OrderType::Market,
            "time_in_force": if request.order_type == OrderType::Market {
                "IMMEDIATE_OR_CANCEL"
            } else {
                "GOOD_TILL_TIME"
            },
            "post_only": request.post_only,
            "reduce_only": false,
            "legs": [{
                "instrument": grvt_symbol_from_internal(&request.symbol),
                "size": quantized_size.normalize().to_string(),
                "limit_price": if request.order_type == OrderType::Market {
                    Value::Null
                } else {
                    Value::String(limit_price.normalize().to_string())
                },
                "is_buying_asset": request.side == Side::Bid
            }],
            "signature": {
                "signer": signer,
                "r": format!("{:#066x}", signature.r),
                "s": format!("{:#066x}", signature.s),
                "v": signature.v as u64,
                "expiration": expiration.to_string(),
                "nonce": nonce,
                "chain_id": self.config.grvt_chain_id
            },
            "metadata": {
                "client_order_id": format!("{nonce}"),
                "create_time": self.now_nanos()?.to_string()
            }
        });

        let has_builder = !self.config.grvt_builder_id.trim().is_empty()
            && self.config.grvt_builder_id.trim() != "0"
            && self.config.grvt_builder_id.trim() != "0x0000000000000000000000000000000000000000"
            && self.config.grvt_builder_fee.trim() != "0";
        if has_builder {
            order["builder"] = Value::String(self.config.grvt_builder_id.clone());
            order["builder_fee"] = Value::String(self.config.grvt_builder_fee.clone());
        }

        Ok(json!({ "order": order }))
    }

    fn now_grvt_nonce(&self) -> u32 {
        // Combine epoch seconds with a monotonic counter to guarantee uniqueness
        // even when multiple orders are placed in the same millisecond.
        let epoch_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let counter = self.nonce_counter.fetch_add(1, Ordering::Relaxed);
        // epoch_secs * 1_000_000 + counter, wrapping into u32 range.
        ((epoch_secs.wrapping_mul(1_000_000)).wrapping_add(counter)) as u32
    }

    fn grvt_chain_id(&self) -> u64 {
        self.config.grvt_chain_id.parse::<u64>().unwrap_or(325)
    }

    fn now_nanos(&self) -> Result<i64> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before unix epoch")?
            .as_nanos();
        i64::try_from(nanos).context("nanoseconds overflow")
    }
}

#[async_trait]
impl ExchangeClient for GrvtClient {
    async fn load_instruments(&self) -> Result<Vec<InstrumentMeta>> {
        Ok(self
            .instruments_by_symbol()
            .await?
            .into_values()
            .map(|instrument| instrument.meta)
            .collect())
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Vec<MarketEvent> {
        let mut events = Vec::new();
        for symbol in symbols {
            let result = self
                .request_policy
                .send_with_retry(&self.rest_governor, "grvt_ticker_snapshot", || {
                    self.http
                        .post(self.market_data_endpoint("/full/v1/ticker"))
                        .json(&json!({ "instrument": grvt_symbol_from_internal(symbol) }))
                        .send()
                })
                .await;

            match result {
                Ok(response) => match response.json::<serde_json::Value>().await {
                    Ok(payload) => {
                        let result = payload.get("result").unwrap_or(&payload);
                        let now = Utc::now();
                        if let (Some(bid), Some(ask)) = (
                            result
                                .get("best_bid_price")
                                .or_else(|| result.get("bb"))
                                .and_then(as_decimal),
                            result
                                .get("best_ask_price")
                                .or_else(|| result.get("ba"))
                                .and_then(as_decimal),
                        ) {
                            events.push(MarketEvent::BestBidAsk {
                                symbol: symbol.clone(),
                                bid,
                                ask,
                                bid_size: result
                                    .get("best_bid_size")
                                    .or_else(|| result.get("bq"))
                                    .and_then(as_decimal),
                                ask_size: result
                                    .get("best_ask_size")
                                    .or_else(|| result.get("aq"))
                                    .and_then(as_decimal),
                                timestamp: now,
                            });
                        }
                        if let Some(funding_rate) = result
                            .get("funding_rate")
                            .or_else(|| result.get("fr"))
                            .or_else(|| result.get("fp"))
                            .and_then(as_decimal)
                        {
                            events.push(MarketEvent::FundingRate {
                                symbol: symbol.clone(),
                                rate: funding_rate,
                                timestamp: now,
                            });
                        }
                        if let Some(mark) = result
                            .get("mark_price")
                            .or_else(|| result.get("mp"))
                            .and_then(as_decimal)
                        {
                            events.push(MarketEvent::MarkPrice {
                                symbol: symbol.clone(),
                                price: mark,
                                timestamp: now,
                            });
                        }
                    }
                    Err(err) => {
                        warn!(symbol = %symbol, error = %err, "grvt REST snapshot: failed to parse response");
                    }
                },
                Err(err) => {
                    warn!(symbol = %symbol, error = %err, "grvt REST market snapshot failed");
                }
            }
        }
        events
    }
}

#[async_trait]
impl MarketDataSource for GrvtClient {
    async fn stream_mark_prices(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let selectors = symbols
            .iter()
            .map(|symbol| format!("{}@100", grvt_symbol_from_internal(symbol)))
            .collect::<Vec<_>>();
        self.stream_market_topic("v1.mini.d", &selectors, sender)
            .await
    }

    async fn stream_spot_prices(
        &self,
        _symbols: &[String],
        _sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        // GRVT mini stream frames already carry mark, index/spot and BBO data together.
        // We subscribe once in `stream_mark_prices` and fan all three event types out there.
        pending::<Result<()>>().await
    }

    async fn stream_best_bid_ask(
        &self,
        _symbols: &[String],
        _sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        pending::<Result<()>>().await
    }

    async fn stream_trades(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let selectors = symbols
            .iter()
            .map(|symbol| format!("{}@50", grvt_symbol_from_internal(symbol)))
            .collect::<Vec<_>>();
        self.stream_market_topic("v1.trade", &selectors, sender)
            .await
    }

    async fn stream_orderbook(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let selectors = symbols
            .iter()
            .map(|symbol| format!("{}@500", grvt_symbol_from_internal(symbol)))
            .collect::<Vec<_>>();
        self.stream_market_topic("v1.book.d", &selectors, sender)
            .await
    }
}

#[async_trait]
impl PrivateDataSource for GrvtClient {
    async fn stream_private_data(&self, sender: mpsc::Sender<PrivateEvent>) -> Result<()> {
        info!("grvt private websocket loop starting");
        self.request_policy
            .run_ws_loop(
                "grvt_private",
                || {
                    let client = self.clone();
                    let sender = sender.clone();
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
        info!("grvt fetching open orders");
        let response = self
            .post_trading(
                "/full/v1/open_orders",
                &json!({ "sub_account_id": self.config.grvt_sub_account_id }),
            )
            .await?;
        let orders = response
            .get("result")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        info!(count = orders.len(), "grvt open orders fetched");
        Ok(orders.iter().filter_map(parse_grvt_open_order).collect())
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>> {
        info!("grvt fetching positions");
        let response = self
            .post_trading(
                "/full/v1/positions",
                &json!({
                    "sub_account_id": self.config.grvt_sub_account_id,
                    "kind": ["PERPETUAL"]
                }),
            )
            .await?;
        let positions = response
            .get("result")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        info!(count = positions.len(), "grvt positions fetched");
        Ok(positions.iter().filter_map(parse_grvt_position).collect())
    }
}

impl GrvtClient {
    async fn run_private_ws_once(&self, sender: mpsc::Sender<PrivateEvent>) -> Result<()> {
        let (cookie, account_id) = self.ensure_session().await?;
        let mut request = self
            .config
            .grvt_trading_ws_url
            .clone()
            .into_client_request()
            .context("invalid grvt trading websocket url")?;
        request
            .headers_mut()
            .insert(COOKIE, HeaderValue::from_str(&cookie)?);
        request
            .headers_mut()
            .insert("X-Grvt-Account-Id", HeaderValue::from_str(&account_id)?);

        let (mut ws_stream, _) = connect_async(request).await?;
        info!("grvt private websocket connected");

        self.send_private_snapshot(&sender).await?;
        let replay_fill_cutoff = Utc::now();
        self.subscribe_private_streams(&mut ws_stream).await?;

        let mut snapshot_interval = {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await; // consume the immediate first tick
            interval
        };
        // Issue 9: keepalive every 20 s (was 10 s).
        let mut keepalive = {
            let mut t = tokio::time::interval(Duration::from_secs(20));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t.tick().await;
            t
        };
        loop {
            tokio::select! {
                _ = snapshot_interval.tick() => {
                    self.send_private_snapshot(&sender).await?;
                }
                _ = keepalive.tick() => {
                    ws_stream.send(WsMessage::Ping(vec![].into())).await?;
                }
                message = ws_stream.next() => {
                    match message {
                        Some(Ok(WsMessage::Text(text))) => {
                            if let Some(error_message) = parse_grvt_ws_error(&text) {
                                warn!(error = %error_message, frame = %text, "grvt private websocket returned error");
                                continue;
                            }
                            let replay_cutoff = replay_fill_cutoff;
                            for event in parse_grvt_private_events(&text)? {
                                // Issue 1/9: drop fills replayed from before this connection.
                                // GRVT replays recent fills on reconnect; the position snapshot
                                // already accounts for them — applying them again doubles position.
                                if let PrivateEvent::Fill(ref fill) = event {
                                    if fill.timestamp <= replay_cutoff {
                                        continue;
                                    }
                                }
                                sender.send(event).await?;
                            }
                        }
                        Some(Ok(WsMessage::Ping(payload))) => {
                            ws_stream.send(WsMessage::Pong(payload)).await?;
                        }
                        Some(Ok(WsMessage::Close(frame))) => {
                            warn!(?frame, "grvt private websocket closed");
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(err)) => return Err(err.into()),
                        None => break,
                    }
                }
            }
        }

        bail!("grvt private websocket closed")
    }

    async fn send_private_snapshot(&self, sender: &mpsc::Sender<PrivateEvent>) -> Result<()> {
        if let Ok(equity) = self.fetch_account_equity().await {
            info!(equity = %equity, "grvt private snapshot account equity fetched");
            sender.send(PrivateEvent::AccountEquity { equity }).await?;
        } else {
            warn!("grvt account equity fetch failed; equity will remain 0");
        }
        let open_orders = self.fetch_open_orders().await?;
        sender.send(PrivateEvent::OpenOrders(open_orders)).await?;
        for position in self.fetch_positions().await? {
            sender.send(PrivateEvent::Position(position)).await?;
        }
        Ok(())
    }

    async fn fetch_account_equity(&self) -> Result<Decimal> {
        let response = self
            .post_trading(
                "/full/v1/account_summary",
                &json!({ "sub_account_id": self.config.grvt_sub_account_id }),
            )
            .await?;
        let result = response.get("result").unwrap_or(&response);
        // GRVT returns total_equity as the net value of the sub-account.
        result
            .get("total_equity")
            .or_else(|| result.get("totalEquity"))
            .or_else(|| result.get("net_equity"))
            .and_then(as_decimal)
            .ok_or_else(|| {
                anyhow::anyhow!("grvt account_summary missing total_equity field: {result}")
            })
    }

    async fn subscribe_private_streams(
        &self,
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<()> {
        let selectors = vec![self.config.grvt_sub_account_id.clone()];
        for (id, stream) in [
            (201u64, "v1.fill"),
            (202u64, "v1.position"),
            (203u64, "v1.order"),
        ] {
            let subscribe = serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "method": "subscribe",
                "params": {
                    "stream": stream,
                    "selectors": selectors,
                },
                "id": id,
            }))?;
            ws_stream.send(WsMessage::Text(subscribe.into())).await?;
            info!(stream, "grvt private websocket subscribed");
        }
        Ok(())
    }
}

#[async_trait]
impl OrderExecutor for GrvtClient {
    async fn place_orders(&self, requests: Vec<OrderRequest>) -> Result<()> {
        let instruments = Arc::new(self.instruments_by_symbol().await?);
        let futs = requests.into_iter().map(|request| {
            let client = self.clone();
            let instruments = Arc::clone(&instruments);
            async move {
                let instrument = instruments
                    .get(&request.symbol)
                    .with_context(|| format!("missing grvt instrument for {}", request.symbol))?;
                let payload = client
                    .build_grvt_order_payload(&request, instrument)
                    .await?;
                let response = match client
                    .post_trading_order("/full/v1/create_order", &payload)
                    .await
                {
                    Ok(response) => response,
                    Err(err) if is_grvt_min_notional_error(&err.to_string()) => {
                        warn!(
                            symbol = %request.symbol,
                            side = ?request.side,
                            order_type = ?request.order_type,
                            quantity = %request.quantity,
                            price = ?request.price,
                            err = %err,
                            "skipping grvt order rejected below minimum notional"
                        );
                        return Ok(());
                    }
                    Err(err) => return Err(err),
                };
                if response.get("error").is_some() {
                    bail!("grvt create_order rejected: {response}");
                }
                Ok::<(), anyhow::Error>(())
            }
        });
        futures_util::future::try_join_all(futs).await?;
        Ok(())
    }

    async fn cancel_orders(&self, order_ids: Vec<String>) -> Result<()> {
        let futs = order_ids.into_iter().map(|order_id| {
            let client = self.clone();
            async move {
                let response = client
                    .post_trading_order(
                        "/full/v1/cancel_order",
                        &json!({
                            "sub_account_id": client.config.grvt_sub_account_id,
                            "order_id": order_id,
                        }),
                    )
                    .await?;
                if response.get("error").is_some() {
                    warn!(response = %response, "grvt cancel_order returned error payload");
                }
                Ok::<(), anyhow::Error>(())
            }
        });
        futures_util::future::try_join_all(futs).await?;
        Ok(())
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        let response = self
            .post_trading(
                "/full/v1/cancel_all_orders",
                &json!({ "sub_account_id": self.config.grvt_sub_account_id }),
            )
            .await?;
        if response.get("error").is_some() {
            bail!("grvt cancel_all_orders rejected: {response}");
        }
        Ok(())
    }
}

fn extract_grvt_cookie(headers: &HeaderMap) -> Option<String> {
    headers.get_all(SET_COOKIE).iter().find_map(|value| {
        let cookie = value.to_str().ok()?;
        cookie
            .split(';')
            .find(|part| part.trim_start().starts_with("gravity="))
            .map(|value| value.trim().to_string())
    })
}

fn extract_grvt_account_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-grvt-account-id")
        .or_else(|| headers.get("X-Grvt-Account-Id"))
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn grvt_request_id(stream_name: &str) -> u64 {
    match stream_name {
        "v1.mini.d" => 101,
        "v1.trade" => 102,
        "v1.book.d" => 103,
        _ => 199,
    }
}

fn grvt_symbol_from_internal(symbol: &str) -> String {
    symbol.replace('/', "_").replace("-P", "_Perp")
}

fn grvt_symbol_to_internal(symbol: &str) -> String {
    symbol.replace("_Perp", "-P").replace('_', "/")
}

fn parse_market_events(stream_name: &str, frame: &str) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(feed) = value.get("feed").or_else(|| value.get("f")) else {
        return Ok(Vec::new());
    };
    let effective_stream = value
        .get("stream")
        .or_else(|| value.get("s"))
        .and_then(Value::as_str)
        .unwrap_or(stream_name);
    let symbol = value
        .get("selector")
        .or_else(|| value.get("s1"))
        .and_then(Value::as_str)
        .map(|selector| selector.split('@').next().unwrap_or(selector).to_string())
        .or_else(|| {
            feed.get("instrument")
                .or_else(|| feed.get("i"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_default();
    if symbol.is_empty() {
        return Ok(Vec::new());
    }
    let internal_symbol = grvt_symbol_to_internal(&symbol);
    let timestamp = parse_grvt_timestamp(feed.get("event_time").or_else(|| feed.get("et")))
        .unwrap_or_else(Utc::now);
    let sequence_number = value
        .get("sequence_number")
        .or_else(|| value.get("sn"))
        .and_then(|value| match value {
            Value::String(raw) => raw.parse::<u64>().ok(),
            Value::Number(raw) => raw.as_u64(),
            _ => None,
        });

    let events = match effective_stream {
        "v1.mini.d" => {
            let mut events = Vec::new();
            if let Some(mark_price) = feed
                .get("mark_price")
                .or_else(|| feed.get("mp"))
                .and_then(as_decimal)
            {
                events.push(MarketEvent::MarkPrice {
                    symbol: internal_symbol.clone(),
                    price: mark_price,
                    timestamp,
                });
            }
            if let Some(index_price) = feed
                .get("index_price")
                .or_else(|| feed.get("ip"))
                .and_then(as_decimal)
            {
                events.push(MarketEvent::SpotPrice {
                    symbol: internal_symbol.clone(),
                    price: index_price,
                    timestamp,
                });
            }
            if let (Some(bid), Some(ask)) = (
                feed.get("best_bid_price")
                    .or_else(|| feed.get("bb"))
                    .and_then(as_decimal),
                feed.get("best_ask_price")
                    .or_else(|| feed.get("ba"))
                    .and_then(as_decimal),
            ) {
                events.push(MarketEvent::BestBidAsk {
                    symbol: internal_symbol.clone(),
                    bid,
                    ask,
                    bid_size: feed
                        .get("best_bid_size")
                        .or_else(|| feed.get("bq"))
                        .and_then(as_decimal),
                    ask_size: feed
                        .get("best_ask_size")
                        .or_else(|| feed.get("aq"))
                        .and_then(as_decimal),
                    timestamp,
                });
            }
            if let Some(funding_rate) = feed
                .get("funding_rate")
                .or_else(|| feed.get("fr"))
                .or_else(|| feed.get("fp"))
                .and_then(as_decimal)
            {
                events.push(MarketEvent::FundingRate {
                    symbol: internal_symbol,
                    rate: funding_rate,
                    timestamp,
                });
            }
            events
        }
        "v1.trade" => {
            let Some(price) = feed
                .get("price")
                .or_else(|| feed.get("p"))
                .and_then(as_decimal)
            else {
                return Ok(Vec::new());
            };
            let Some(quantity) = feed
                .get("size")
                .or_else(|| feed.get("s"))
                .and_then(as_decimal)
            else {
                return Ok(Vec::new());
            };
            vec![MarketEvent::Trade {
                symbol: internal_symbol,
                price,
                quantity,
                taker_side: feed
                    .get("is_taker_buyer")
                    .or_else(|| feed.get("it"))
                    .and_then(Value::as_bool)
                    .map(|is_buyer| if is_buyer { Side::Bid } else { Side::Ask }),
                timestamp,
            }]
        }
        "v1.book.d" => {
            let bids = parse_grvt_book_side(feed.get("bids").or_else(|| feed.get("b")))?;
            let asks = parse_grvt_book_side(feed.get("asks").or_else(|| feed.get("a")))?;
            if sequence_number == Some(0) {
                vec![MarketEvent::OrderBookSnapshot {
                    symbol: internal_symbol,
                    bids,
                    asks,
                    timestamp,
                }]
            } else {
                vec![MarketEvent::OrderBookUpdate {
                    symbol: internal_symbol,
                    bids,
                    asks,
                    timestamp,
                }]
            }
        }
        _ => Vec::new(),
    };

    Ok(events)
}

fn parse_grvt_ws_error(frame: &str) -> Option<String> {
    let value: Value = serde_json::from_str(frame).ok()?;
    let error = value.get("error").or_else(|| value.get("e"))?;
    let code = error
        .get("code")
        .or_else(|| error.get("c"))
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let message = error
        .get("message")
        .or_else(|| error.get("m"))
        .and_then(Value::as_str)
        .unwrap_or("unknown websocket error");
    Some(format!("code={code} message={message}"))
}

fn parse_grvt_book_side(value: Option<&Value>) -> Result<BTreeMap<Decimal, Decimal>> {
    let mut output = BTreeMap::new();
    let Some(items) = value.and_then(Value::as_array) else {
        return Ok(output);
    };
    for item in items {
        let Some(price) = item
            .get("price")
            .or_else(|| item.get("p"))
            .and_then(as_decimal)
        else {
            continue;
        };
        let Some(size) = item
            .get("size")
            .or_else(|| item.get("s"))
            .and_then(as_decimal)
        else {
            continue;
        };
        output.insert(price, size);
    }
    Ok(output)
}

fn parse_grvt_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let raw = value?.as_str()?.parse::<i64>().ok()?;
    let seconds = raw / 1_000_000_000;
    let nanos = (raw % 1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
}

fn parse_grvt_open_order(value: &Value) -> Option<OpenOrder> {
    let leg = value.get("legs")?.as_array()?.first()?;
    let book_size = value
        .get("state")
        .and_then(|state| state.get("book_size").or_else(|| state.get("bs")))
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(as_decimal)
        .or_else(|| {
            leg.get("size")
                .or_else(|| leg.get("s"))
                .and_then(as_decimal)
        })
        .unwrap_or(Decimal::ZERO);
    Some(OpenOrder {
        order_id: value
            .get("order_id")
            .or_else(|| value.get("oi"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        nonce: value
            .get("signature")
            .and_then(|signature| signature.get("nonce").or_else(|| signature.get("n")))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        level_index: None,
        symbol: grvt_symbol_to_internal(
            leg.get("instrument")
                .or_else(|| leg.get("i"))
                .and_then(Value::as_str)?,
        ),
        side: if leg
            .get("is_buying_asset")
            .or_else(|| leg.get("ib"))
            .and_then(Value::as_bool)?
        {
            Side::Bid
        } else {
            Side::Ask
        },
        price: leg
            .get("limit_price")
            .or_else(|| leg.get("lp"))
            .and_then(as_decimal),
        remaining_quantity: book_size,
    })
}

/// Convert a `v1.order` WS feed item into a `PrivateEvent`.
/// OPEN / PARTIALLY_FILLED → UpsertOpenOrder (order is still live on book).
/// FILLED / CANCELLED / REJECTED / anything else → RemoveOpenOrder.
fn parse_grvt_order_event(value: &Value) -> Option<PrivateEvent> {
    let status = value
        .get("state")
        .and_then(|s| s.get("status").or_else(|| s.get("s")))
        .and_then(Value::as_str)
        .unwrap_or("OPEN");
    match status {
        "OPEN" | "PARTIALLY_FILLED" => {
            parse_grvt_open_order(value).map(PrivateEvent::UpsertOpenOrder)
        }
        _ => {
            let order_id = value
                .get("order_id")
                .or_else(|| value.get("oi"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let nonce = value
                .get("signature")
                .and_then(|s| s.get("nonce").or_else(|| s.get("n")))
                .and_then(Value::as_u64);
            Some(PrivateEvent::RemoveOpenOrder { order_id, nonce })
        }
    }
}

fn parse_grvt_position(value: &Value) -> Option<Position> {
    let instrument = value
        .get("instrument")
        .or_else(|| value.get("i"))
        .and_then(Value::as_str)?;
    let quantity = value
        .get("size")
        .or_else(|| value.get("s"))
        .and_then(as_decimal)?;
    Some(Position {
        symbol: grvt_symbol_to_internal(instrument),
        quantity,
        entry_price: value
            .get("entry_price")
            .or_else(|| value.get("ep"))
            .and_then(as_decimal)
            .or_else(|| {
                value
                    .get("avg_entry_price")
                    .or_else(|| value.get("aep"))
                    .and_then(as_decimal)
            })
            .unwrap_or(Decimal::ZERO),
        realized_pnl: value
            .get("realized_pnl")
            .or_else(|| value.get("rp"))
            .or_else(|| value.get("realised_pnl"))
            .and_then(as_decimal)
            .unwrap_or(Decimal::ZERO),
        unrealized_pnl: value
            .get("unrealized_pnl")
            .or_else(|| value.get("up"))
            .or_else(|| value.get("unrealised_pnl"))
            .and_then(as_decimal)
            .unwrap_or(Decimal::ZERO),
        pnl_is_authoritative: value
            .get("realized_pnl")
            .or_else(|| value.get("rp"))
            .or_else(|| value.get("realised_pnl"))
            .and_then(as_decimal)
            .map(|pnl| !pnl.is_zero())
            .unwrap_or(false),
        opened_at: None,
    })
}

fn parse_grvt_fill(value: &Value) -> Option<crate::domain::Fill> {
    let instrument = value
        .get("instrument")
        .or_else(|| value.get("i"))
        .and_then(Value::as_str)?;
    let quantity = value
        .get("size")
        .or_else(|| value.get("s"))
        .and_then(as_decimal)?;
    let price = value
        .get("price")
        .or_else(|| value.get("p"))
        .and_then(as_decimal)?;
    let is_buyer = value
        .get("is_buyer")
        .or_else(|| value.get("ib"))
        .and_then(Value::as_bool)?;
    let timestamp = parse_grvt_timestamp(value.get("event_time").or_else(|| value.get("et")))
        .unwrap_or_else(Utc::now);
    Some(crate::domain::Fill {
        order_id: value
            .get("order_id")
            .or_else(|| value.get("oid"))
            .and_then(Value::as_str)
            .map(ToString::to_string),
        nonce: value
            .get("signature")
            .or_else(|| value.get("sig"))
            .and_then(|signature| signature.get("nonce").or_else(|| signature.get("n")))
            .and_then(Value::as_u64),
        symbol: grvt_symbol_to_internal(instrument),
        side: if is_buyer { Side::Bid } else { Side::Ask },
        price,
        quantity,
        timestamp,
    })
}

fn parse_grvt_private_events(frame: &str) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(feed) = value.get("feed").or_else(|| value.get("f")) else {
        return Ok(Vec::new());
    };
    let stream = value
        .get("stream")
        .or_else(|| value.get("s"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let events = match stream {
        "v1.fill" => {
            if let Some(arr) = feed.as_array() {
                arr.iter()
                    .filter_map(parse_grvt_fill)
                    .map(PrivateEvent::Fill)
                    .collect()
            } else {
                parse_grvt_fill(feed)
                    .map(PrivateEvent::Fill)
                    .into_iter()
                    .collect()
            }
        }
        "v1.position" => {
            if let Some(arr) = feed.as_array() {
                arr.iter()
                    .filter_map(parse_grvt_position)
                    .map(PrivateEvent::Position)
                    .collect()
            } else {
                parse_grvt_position(feed)
                    .map(PrivateEvent::Position)
                    .into_iter()
                    .collect()
            }
        }
        "v1.order" => {
            if let Some(arr) = feed.as_array() {
                arr.iter().filter_map(parse_grvt_order_event).collect()
            } else {
                parse_grvt_order_event(feed).into_iter().collect()
            }
        }
        _ => Vec::new(),
    };
    Ok(events)
}

fn as_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(value) => Decimal::from_str(value).ok(),
        Value::Number(value) => Decimal::from_str(&value.to_string()).ok(),
        _ => None,
    }
}

fn sign_grvt_order(
    wallet: &LocalWallet,
    sub_account_id: &str,
    request: &OrderRequest,
    instrument: &GrvtInstrument,
    nonce: u32,
    expiration: i64,
    chain_id: u64,
) -> Result<EvmSignature> {
    let contract_size = grvt_contract_size(request.quantity, instrument.base_decimals)?;
    let limit_price = if request.order_type == OrderType::Market {
        0u64
    } else {
        grvt_limit_price(request.price.unwrap_or(Decimal::ZERO))?
    };
    let asset_id = U256::from_str_radix(instrument.instrument_hash.trim_start_matches("0x"), 16)?;
    let leg_typehash = keccak256(
        "OrderLeg(uint256 assetID,uint64 contractSize,uint64 limitPrice,bool isBuyingContract)",
    );
    let leg_hash = keccak256(encode(&[
        Token::FixedBytes(leg_typehash.to_vec()),
        Token::Uint(asset_id),
        Token::Uint(U256::from(contract_size)),
        Token::Uint(U256::from(limit_price)),
        Token::Bool(request.side == Side::Bid),
    ]));
    let legs_hash = keccak256(leg_hash);
    let order_typehash = keccak256(
        "Order(uint64 subAccountID,bool isMarket,uint8 timeInForce,bool postOnly,bool reduceOnly,OrderLeg[] legs,uint32 nonce,int64 expiration)OrderLeg(uint256 assetID,uint64 contractSize,uint64 limitPrice,bool isBuyingContract)",
    );
    let order_hash = keccak256(encode(&[
        Token::FixedBytes(order_typehash.to_vec()),
        Token::Uint(U256::from(sub_account_id.parse::<u64>()?)),
        Token::Bool(request.order_type == OrderType::Market),
        Token::Uint(U256::from(if request.order_type == OrderType::Market {
            3u64
        } else {
            1u64
        })),
        Token::Bool(request.post_only),
        Token::Bool(false),
        Token::FixedBytes(legs_hash.to_vec()),
        Token::Uint(U256::from(nonce)),
        Token::Int(U256::from(expiration as u64)),
    ]));
    let digest = grvt_eip712_digest(chain_id, order_hash);
    wallet
        .sign_hash(digest)
        .context("grvt order signing failed")
}

fn sign_grvt_wallet_login(
    wallet: &LocalWallet,
    signer: &str,
    nonce: u32,
    expiration: i64,
    chain_id: u64,
) -> Result<EvmSignature> {
    let signer_address = signer
        .parse::<Address>()
        .context("invalid GRVT signer address")?;
    let typehash = keccak256("WalletLogin(address signer,uint32 nonce,int64 expiration)");
    let struct_hash = keccak256(encode(&[
        Token::FixedBytes(typehash.to_vec()),
        Token::Address(signer_address),
        Token::Uint(U256::from(nonce)),
        Token::Int(U256::from(expiration as u64)),
    ]));
    let digest = grvt_eip712_digest(chain_id, struct_hash);
    wallet
        .sign_hash(digest)
        .context("grvt wallet login manual signing failed")
}

fn grvt_eip712_digest(chain_id: u64, struct_hash: [u8; 32]) -> H256 {
    let domain_separator = grvt_domain_separator(chain_id);
    let digest = keccak256(
        [
            [0x19, 0x01].as_slice(),
            domain_separator.as_slice(),
            struct_hash.as_slice(),
        ]
        .concat(),
    );
    H256::from(digest)
}

fn grvt_domain_separator(chain_id: u64) -> [u8; 32] {
    let typehash = keccak256("EIP712Domain(string name,string version,uint256 chainId)");
    let name_hash = keccak256("GRVT Exchange");
    let version_hash = keccak256("0");
    keccak256(encode(&[
        Token::FixedBytes(typehash.to_vec()),
        Token::FixedBytes(name_hash.to_vec()),
        Token::FixedBytes(version_hash.to_vec()),
        Token::Uint(U256::from(chain_id)),
    ]))
}

fn grvt_contract_size(quantity: Decimal, base_decimals: i32) -> Result<u64> {
    let scaled = if base_decimals >= 0 {
        quantity * pow10_decimal(base_decimals as u32)
    } else {
        quantity / pow10_decimal((-base_decimals) as u32)
    };
    scaled
        .trunc()
        .to_u64()
        .ok_or_else(|| anyhow!("grvt contract size overflow"))
}

fn grvt_quantize_size(quantity: Decimal, base_decimals: i32, min_size: Decimal) -> Decimal {
    let decimal_step = if base_decimals >= 0 {
        Decimal::ONE / pow10_decimal(base_decimals as u32)
    } else {
        pow10_decimal((-base_decimals) as u32)
    };
    let size_step = if min_size > decimal_step {
        min_size
    } else {
        decimal_step
    };
    let aligned = align_to_step(quantity, size_step);
    if aligned.is_sign_positive() && aligned < min_size {
        min_size
    } else {
        aligned
    }
}

fn grvt_limit_price(price: Decimal) -> Result<u64> {
    (price * Decimal::from(GRVT_PRICE_MULTIPLIER))
        .trunc()
        .to_u64()
        .ok_or_else(|| anyhow!("grvt limit price overflow"))
}

fn grvt_quantize_price(price: Decimal, tick_size: Option<Decimal>) -> Decimal {
    match tick_size {
        Some(step) if step > Decimal::ZERO => align_to_step(price, step),
        _ => price,
    }
}

fn is_grvt_min_notional_error(message: &str) -> bool {
    message.contains("\"code\":2066")
        || message.contains("Order below minimum notional")
        || message.contains("below minimum notional")
}

fn cleanup_limit_price(
    side: Side,
    best_bid: Decimal,
    best_ask: Decimal,
    tick_size: Option<Decimal>,
) -> Decimal {
    let Some(step) = tick_size.filter(|step| *step > Decimal::ZERO) else {
        return match side {
            Side::Bid => best_bid,
            Side::Ask => best_ask,
        };
    };

    match side {
        Side::Bid => {
            let improved = best_bid + step;
            if improved < best_ask {
                improved
            } else {
                best_bid
            }
        }
        Side::Ask => {
            let improved = best_ask - step;
            if improved > best_bid {
                improved
            } else {
                best_ask
            }
        }
    }
}

fn align_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).trunc() * step
}

fn pow10_decimal(exp: u32) -> Decimal {
    let mut out = Decimal::ONE;
    for _ in 0..exp {
        out *= Decimal::from(10u64);
    }
    out
}

#[derive(Debug, Deserialize)]
struct GrvtAllInstrumentsResponse {
    result: Vec<GrvtInstrumentDisplay>,
}

#[derive(Debug, Deserialize)]
struct GrvtServerTimeResponse {
    server_time: u64,
}

#[derive(Debug, Deserialize)]
struct GrvtInstrumentDisplay {
    instrument: String,
    instrument_hash: String,
    kind: String,
    venues: Vec<String>,
    base_decimals: i32,
    min_size: Decimal,
    tick_size: Decimal,
}
