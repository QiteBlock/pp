use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, USER_AGENT},
    Client,
};
use rust_crypto_lib_base::{
    sign_message,
    starknet_messages::{AssetId, OffChainMessage, Order, PositionId, StarknetDomain, Timestamp},
};
use rust_decimal::{prelude::ToPrimitive, Decimal, RoundingStrategy};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};
use starknet_crypto::{get_public_key, Felt};
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
        PrivateEvent, Side, TimeInForce,
    },
};

const X_API_KEY: &str = "x-api-key";

#[derive(Clone)]
pub struct ExtendedClient {
    config: VenueConfig,
    http: Client,
    rest_governor: Arc<RestGovernor>,
    request_policy: RequestPolicy,
    instrument_cache: Arc<RwLock<Option<HashMap<String, InstrumentMeta>>>>,
    market_cache: Arc<RwLock<Option<HashMap<String, ExtendedMarket>>>>,
    account_cache: Arc<RwLock<Option<ExtendedAccountIdentity>>>,
}

impl ExtendedClient {
    pub fn new(config: VenueConfig, network: &NetworkConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(X_API_KEY),
            HeaderValue::from_str(&config.api_key).context("invalid Extended API key")?,
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
            market_cache: Arc::new(RwLock::new(None)),
            account_cache: Arc::new(RwLock::new(None)),
        })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.config.api_base_url.trim_end_matches('/'), path)
    }

    fn ws_market_endpoint(&self, path: &str) -> String {
        format!(
            "{}{}",
            self.config.ws_market_url.trim_end_matches('/'),
            path
        )
    }

    fn ws_account_endpoint(&self, path: &str) -> String {
        format!(
            "{}{}",
            self.config.ws_account_url.trim_end_matches('/'),
            path
        )
    }

    async fn load_markets_uncached(&self) -> Result<HashMap<String, ExtendedMarket>> {
        let response: ExtendedResponse<Vec<ExtendedMarket>> = self
            .request_policy
            .send_with_retry(&self.rest_governor, "extended_load_markets", || {
                self.http.get(self.endpoint("/api/v1/info/markets")).send()
            })
            .await?
            .json()
            .await?;

        Ok(response
            .data
            .into_iter()
            .map(|market| (market.market.clone(), market))
            .collect())
    }

    async fn markets_by_symbol(&self) -> Result<HashMap<String, ExtendedMarket>> {
        if let Some(cache) = self.market_cache.read().await.as_ref() {
            return Ok(cache.clone());
        }

        let by_symbol = self.load_markets_uncached().await?;
        *self.market_cache.write().await = Some(by_symbol.clone());
        Ok(by_symbol)
    }

    async fn instruments_by_symbol(&self) -> Result<HashMap<String, InstrumentMeta>> {
        if let Some(cache) = self.instrument_cache.read().await.as_ref() {
            return Ok(cache.clone());
        }

        let by_symbol: HashMap<String, InstrumentMeta> = self
            .markets_by_symbol()
            .await?
            .into_values()
            .map(|market| {
                let instrument = InstrumentMeta {
                    symbol: market.market.clone(),
                    contract_id: 0,
                    underlying_decimals: decimals_from_resolution(
                        market.l2_config.synthetic_resolution,
                    ),
                    settlement_decimals: decimals_from_resolution(
                        market.l2_config.collateral_resolution,
                    ),
                    min_order_size: parse_decimal_string(&market.trading_config.min_order_size)?,
                    tick_size: Some(parse_decimal_string(
                        &market.trading_config.min_price_change,
                    )?),
                };
                Ok((instrument.symbol.clone(), instrument))
            })
            .collect::<Result<_>>()?;
        *self.instrument_cache.write().await = Some(by_symbol.clone());
        Ok(by_symbol)
    }

    async fn account_identity(&self) -> Result<ExtendedAccountIdentity> {
        if let Some(cache) = self.account_cache.read().await.as_ref() {
            return Ok(cache.clone());
        }

        let private_key = Felt::from_hex(&self.config.private_key)
            .context("invalid Extended Stark private key hex")?;
        let derived_public_key = get_public_key(&private_key);

        let response: ExtendedResponse<ExtendedAccountDetails> = self
            .request_policy
            .send_with_retry(&self.rest_governor, "extended_fetch_account", || {
                self.http
                    .get(self.endpoint("/api/v1/user/account/info"))
                    .send()
            })
            .await?
            .json()
            .await?;
        let api_public_key = Felt::from_hex(&response.data.l2_key)
            .context("invalid Extended l2Key public key hex")?;
        if api_public_key != derived_public_key {
            bail!(
                "Extended private_key does not match account l2Key from API management (derived={}, api={})",
                derived_public_key.to_hex_string(),
                response.data.l2_key
            );
        }

        let vault = if self.config.account_id != 0 {
            self.config.account_id
        } else {
            response.data.l2_vault
        };
        if vault == 0 {
            bail!("Extended account is missing l2Vault/account_id for settlement");
        }

        let identity = ExtendedAccountIdentity {
            vault,
            public_key_hex: response.data.l2_key,
            public_key: api_public_key,
            private_key,
        };
        *self.account_cache.write().await = Some(identity.clone());
        Ok(identity)
    }

    async fn fetch_balance_equity(&self) -> Result<Decimal> {
        let response = self
            .request_policy
            .send_with_retry_allow_status(&self.rest_governor, "extended_fetch_balance", || {
                self.http.get(self.endpoint("/api/v1/user/balance")).send()
            })
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Decimal::ZERO);
        }
        let response: ExtendedResponse<ExtendedBalanceSnapshot> = response.json().await?;
        parse_decimal_string(&response.data.equity)
    }

    async fn fetch_funding_payments_since(
        &self,
        symbols: &[String],
        start_time: chrono::DateTime<Utc>,
    ) -> Result<Vec<FundingPaymentEvent>> {
        let endpoint = self.endpoint("/api/v1/user/funding/history");
        let mut cursor: Option<String> = None;
        let mut events = Vec::new();

        loop {
            let mut url =
                reqwest::Url::parse(&endpoint).context("invalid Extended funding history url")?;
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("startTime", &start_time.timestamp_millis().to_string());
                for symbol in symbols {
                    query.append_pair("market", symbol);
                }
                if let Some(cursor_value) = cursor.as_deref() {
                    query.append_pair("cursor", cursor_value);
                }
            }

            let response: ExtendedFundingHistoryResponse = self
                .request_policy
                .send_with_retry(
                    &self.rest_governor,
                    "extended_fetch_funding_history",
                    || {
                        let url = url.clone();
                        self.http.get(url).send()
                    },
                )
                .await?
                .json()
                .await?;

            for payment in response.data {
                let Some(ts) = Utc.timestamp_millis_opt(payment.paid_time).single() else {
                    continue;
                };
                let amount = parse_decimal_string(&payment.funding_fee)?;
                events.push(FundingPaymentEvent {
                    ts,
                    symbol: Some(payment.market.clone()),
                    payment_type: "funding_payment".to_string(),
                    amount,
                    note: Some(
                        json!({
                            "venue": "extended",
                            "record_id": payment.id,
                            "account_id": payment.account_id,
                            "position_id": payment.position_id,
                            "side": payment.side,
                            "size": payment.size,
                            "value": payment.value,
                            "mark_price": payment.mark_price,
                            "funding_rate": payment.funding_rate,
                        })
                        .to_string(),
                    ),
                    is_simulated: false,
                });
            }

            let next_cursor = response
                .pagination
                .and_then(|pagination| pagination.cursor)
                .filter(|cursor_value| !cursor_value.is_empty());
            if next_cursor.is_none() {
                break;
            }
            cursor = next_cursor;
        }

        Ok(events)
    }

    async fn run_public_ws_loop(
        &self,
        stream_name: &'static str,
        path: String,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
        parser: fn(&str, &HashSet<String>) -> Result<Vec<MarketEvent>>,
    ) -> Result<()> {
        let subscribed: HashSet<String> = symbols.iter().cloned().collect();
        self.request_policy
            .run_ws_loop(
                stream_name,
                || {
                    let path = path.clone();
                    let sender = sender.clone();
                    let subscribed = subscribed.clone();
                    async move {
                        let mut request = path.into_client_request()?;
                        request.headers_mut().insert(
                            USER_AGENT,
                            HeaderValue::from_str("market-making-bot/0.1.0")?,
                        );
                        let (mut ws_stream, _) = connect_async(request).await?;
                        let mut logged_first_text = false;
                        let mut logged_first_event = false;
                        loop {
                            match ws_stream.next().await {
                                Some(Ok(WsMessage::Text(text))) => {
                                    if !logged_first_text {
                                        logged_first_text = true;
                                    }
                                    let events = parser(&text, &subscribed)?;
                                    if !events.is_empty() && !logged_first_event {
                                        logged_first_event = true;
                                    }
                                    for event in events {
                                        sender.send(event).await?;
                                    }
                                }
                                Some(Ok(WsMessage::Ping(payload))) => {
                                    ws_stream.send(WsMessage::Pong(payload)).await?;
                                }
                                Some(Ok(WsMessage::Close(frame))) => {
                                    warn!(
                                        stream = stream_name,
                                        ?frame,
                                        "extended websocket closed"
                                    );
                                    break;
                                }
                                Some(Ok(_)) => {}
                                Some(Err(err)) => return Err(err.into()),
                                None => break,
                            }
                        }
                        bail!("extended websocket closed")
                    }
                },
                || {
                    let sender = sender.clone();
                    let symbols = symbols.to_vec();
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

        let mut request = self
            .ws_account_endpoint("/stream.extended.exchange/v1/account")
            .into_client_request()
            .context("invalid Extended account websocket url")?;
        request.headers_mut().insert(
            HeaderName::from_static(X_API_KEY),
            HeaderValue::from_str(&self.config.api_key)?,
        );
        request
            .headers_mut()
            .insert(USER_AGENT, HeaderValue::from_str(&self.config.user_agent)?);

        let (mut ws_stream, _) = connect_async(request).await?;
        let mut logged_first_text = false;
        let mut logged_first_event = false;

        loop {
            match ws_stream.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    if !logged_first_text {
                        logged_first_text = true;
                    }
                    let events = parse_extended_private_events(&text)?;
                    if !events.is_empty() && !logged_first_event {
                        logged_first_event = true;
                    }
                    for event in events {
                        sender.send(event).await?;
                    }
                }
                Some(Ok(WsMessage::Ping(payload))) => {
                    ws_stream.send(WsMessage::Pong(payload)).await?;
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    warn!(?frame, "extended private websocket closed");
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(err.into()),
                None => break,
            }
        }

        bail!("extended private websocket closed")
    }

    async fn send_private_snapshot(&self, sender: &mpsc::Sender<PrivateEvent>) -> Result<()> {
        let equity = self.fetch_balance_equity().await?;
        sender.send(PrivateEvent::AccountEquity { equity }).await?;
        let open_orders = self.fetch_open_orders().await?;
        sender.send(PrivateEvent::OpenOrders(open_orders)).await?;
        let positions = self.fetch_positions().await?;
        for position in positions {
            sender.send(PrivateEvent::Position(position)).await?;
        }
        Ok(())
    }

    fn build_order_payload(
        &self,
        request: &OrderRequest,
        market: &ExtendedMarket,
        account: &ExtendedAccountIdentity,
        maker_fee_rate: Decimal,
        taker_fee_rate: Decimal,
    ) -> Result<Value> {
        let price = request
            .price
            .with_context(|| format!("Extended order for {} is missing price", request.symbol))?;
        let expiry = Utc::now() + ChronoDuration::hours(1);
        let expiry_epoch_millis = expiry.timestamp_millis();
        let settlement_expiration = ceil_timestamp_millis_to_seconds(
            (expiry + ChronoDuration::days(14)).timestamp_millis(),
        );
        let min_order_size = parse_decimal_string(&market.trading_config.min_order_size)?;
        let min_order_size_change =
            parse_decimal_string(&market.trading_config.min_order_size_change)?;
        let fee_rate = if request.post_only {
            maker_fee_rate
        } else {
            taker_fee_rate
        };
        let nonce = generate_extended_nonce();
        let synthetic_amount =
            quantize_down_to_step(request.quantity.abs(), min_order_size_change)?;
        if synthetic_amount < min_order_size {
            bail!(
                "Extended order quantity {} is below market minimum {} for {}",
                synthetic_amount,
                min_order_size,
                request.symbol
            );
        }
        let collateral_amount = synthetic_amount * price;
        let total_fee = fee_rate * collateral_amount;
        let is_buy = request.side == Side::Bid;

        let synthetic_stark = round_stark_amount(
            synthetic_amount,
            market.l2_config.synthetic_resolution,
            if is_buy {
                RoundingStrategy::ToPositiveInfinity
            } else {
                RoundingStrategy::ToZero
            },
        )?;
        let collateral_stark_abs = round_stark_amount(
            collateral_amount,
            market.l2_config.collateral_resolution,
            if is_buy {
                RoundingStrategy::ToPositiveInfinity
            } else {
                RoundingStrategy::ToZero
            },
        )?;
        let fee_stark = round_stark_amount(
            total_fee,
            market.l2_config.collateral_resolution,
            RoundingStrategy::ToPositiveInfinity,
        )?;

        let base_amount = if is_buy {
            synthetic_stark
        } else {
            -synthetic_stark
        };
        let quote_amount = if is_buy {
            -collateral_stark_abs
        } else {
            collateral_stark_abs
        };

        let order = Order {
            position_id: PositionId {
                value: u32::try_from(account.vault)
                    .context("Extended vault does not fit in u32")?,
            },
            base_asset_id: AssetId {
                value: Felt::from_hex(&market.l2_config.synthetic_id)
                    .context("invalid Extended synthetic asset id")?,
            },
            base_amount: i64::try_from(base_amount).context("Extended base amount overflow")?,
            quote_asset_id: AssetId {
                value: Felt::from_hex(&market.l2_config.collateral_id)
                    .context("invalid Extended collateral asset id")?,
            },
            quote_amount: i64::try_from(quote_amount).context("Extended quote amount overflow")?,
            fee_asset_id: AssetId {
                value: Felt::from_hex(&market.l2_config.collateral_id)
                    .context("invalid Extended fee asset id")?,
            },
            fee_amount: u64::try_from(fee_stark.max(0)).context("Extended fee amount overflow")?,
            expiration: Timestamp {
                seconds: u64::try_from(settlement_expiration)
                    .context("Extended settlement expiration overflow")?,
            },
            salt: u64::try_from(nonce)
                .context("Extended nonce overflow")?
                .into(),
        };
        let domain = extended_starknet_domain(&self.config.api_base_url);
        let msg_hash = order
            .message_hash(&domain, account.public_key)
            .map_err(|err| anyhow::anyhow!("failed to hash Extended order message: {err}"))?;
        let signature = sign_message(&msg_hash, &account.private_key)
            .map_err(|err| anyhow::anyhow!("failed to sign Extended order: {err}"))?;

        Ok(json!({
            "id": build_extended_external_id(request),
            "market": request.symbol,
            "type": match request.order_type {
                crate::domain::OrderType::Limit => "LIMIT",
                crate::domain::OrderType::Market => "MARKET",
            },
            "side": match request.side {
                Side::Bid => "BUY",
                Side::Ask => "SELL",
            },
            "qty": format_decimal(synthetic_amount),
            "price": format_decimal(price),
            "reduceOnly": false,
            "postOnly": request.post_only,
            "timeInForce": match request.time_in_force {
                TimeInForce::GoodTillTime => "GTT",
                TimeInForce::ImmediateOrCancel => "IOC",
            },
            "expiryEpochMillis": expiry_epoch_millis,
            "fee": format_decimal(fee_rate),
            "nonce": nonce.to_string(),
            "selfTradeProtectionLevel": "ACCOUNT",
            "settlement": {
                "signature": {
                    "r": signature.r.to_hex_string(),
                    "s": signature.s.to_hex_string(),
                },
                "starkKey": account.public_key_hex.clone(),
                "collateralPosition": account.vault.to_string(),
            }
        }))
    }
}

#[async_trait]
impl ExchangeClient for ExtendedClient {
    async fn load_instruments(&self) -> Result<Vec<InstrumentMeta>> {
        Ok(self.instruments_by_symbol().await?.into_values().collect())
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Vec<MarketEvent> {
        let mut events = Vec::new();
        for symbol in symbols {
            let response = self
                .request_policy
                .send_with_retry(&self.rest_governor, "extended_market_snapshot", || {
                    self.http
                        .get(self.endpoint("/api/v1/info/markets"))
                        .query(&[("market", symbol.as_str())])
                        .send()
                })
                .await;
            let Ok(response) = response else {
                continue;
            };
            let Ok(payload) = response
                .json::<ExtendedResponse<Vec<ExtendedMarket>>>()
                .await
            else {
                continue;
            };
            let Some(market) = payload.data.into_iter().next() else {
                continue;
            };
            let ts = Utc::now();
            if let (Ok(bid), Ok(ask)) = (
                parse_decimal_string(&market.market_stats.bid_price),
                parse_decimal_string(&market.market_stats.ask_price),
            ) {
                events.push(MarketEvent::BestBidAsk {
                    symbol: market.market.clone(),
                    bid,
                    ask,
                    bid_size: None,
                    ask_size: None,
                    timestamp: ts,
                });
            }
            if let Ok(mark) = parse_decimal_string(&market.market_stats.mark_price) {
                events.push(MarketEvent::MarkPrice {
                    symbol: market.market.clone(),
                    price: mark,
                    timestamp: ts,
                });
            }
            if let Ok(index) = parse_decimal_string(&market.market_stats.index_price) {
                events.push(MarketEvent::SpotPrice {
                    symbol: market.market,
                    price: index,
                    timestamp: ts,
                });
            }
        }
        events
    }

    async fn fetch_funding_payments(
        &self,
        symbols: &[String],
        start_time: chrono::DateTime<Utc>,
    ) -> Result<Vec<FundingPaymentEvent>> {
        self.fetch_funding_payments_since(symbols, start_time).await
    }
}

#[async_trait]
impl MarketDataSource for ExtendedClient {
    async fn stream_mark_prices(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.run_public_ws_loop(
            "extended_mark_price",
            self.ws_market_endpoint("/stream.extended.exchange/v1/prices/mark"),
            symbols,
            sender,
            parse_extended_mark_price_events,
        )
        .await
    }

    async fn stream_spot_prices(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.run_public_ws_loop(
            "extended_index_price",
            self.ws_market_endpoint("/stream.extended.exchange/v1/prices/index"),
            symbols,
            sender,
            parse_extended_index_price_events,
        )
        .await
    }

    async fn stream_best_bid_ask(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.run_public_ws_loop(
            "extended_best_bid_ask",
            self.ws_market_endpoint("/stream.extended.exchange/v1/orderbooks?depth=1"),
            symbols,
            sender,
            parse_extended_bbo_events,
        )
        .await
    }

    async fn stream_trades(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.run_public_ws_loop(
            "extended_trades",
            self.ws_market_endpoint("/stream.extended.exchange/v1/publicTrades"),
            symbols,
            sender,
            parse_extended_trade_events,
        )
        .await
    }

    async fn stream_orderbook(
        &self,
        symbols: &[String],
        sender: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        self.run_public_ws_loop(
            "extended_orderbook",
            self.ws_market_endpoint("/stream.extended.exchange/v1/orderbooks"),
            symbols,
            sender,
            parse_extended_orderbook_events,
        )
        .await
    }
}

#[async_trait]
impl PrivateDataSource for ExtendedClient {
    async fn stream_private_data(&self, sender: mpsc::Sender<PrivateEvent>) -> Result<()> {
        self.request_policy
            .run_ws_loop(
                "extended_private",
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
        let response: ExtendedResponse<Vec<ExtendedOrder>> = self
            .request_policy
            .send_with_retry(&self.rest_governor, "extended_fetch_open_orders", || {
                self.http.get(self.endpoint("/api/v1/user/orders")).send()
            })
            .await?
            .json()
            .await?;
        Ok(response
            .data
            .into_iter()
            .filter_map(|order| parse_extended_open_order(&order))
            .collect())
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>> {
        let response: ExtendedResponse<Vec<ExtendedPosition>> = self
            .request_policy
            .send_with_retry(&self.rest_governor, "extended_fetch_positions", || {
                self.http
                    .get(self.endpoint("/api/v1/user/positions"))
                    .send()
            })
            .await?
            .json()
            .await?;
        Ok(response
            .data
            .into_iter()
            .filter_map(|position| parse_extended_position(&position))
            .collect())
    }
}

#[async_trait]
impl OrderExecutor for ExtendedClient {
    async fn place_orders(&self, requests: Vec<OrderRequest>) -> Result<()> {
        let maker_fee_rate = parse_decimal_string(&self.config.maker_fee_ratio)?;
        let taker_fee_rate = parse_decimal_string(&self.config.max_fee_rate)?;
        let markets = self.markets_by_symbol().await?;
        let account = self.account_identity().await?;

        for request in requests {
            let market = markets
                .get(&request.symbol)
                .with_context(|| format!("unknown Extended market {}", request.symbol))?;
            let payload = self.build_order_payload(
                &request,
                market,
                &account,
                maker_fee_rate,
                taker_fee_rate,
            )?;
            let response = self
                .request_policy
                .send_with_retry_allow_status(&self.rest_governor, "extended_place_order", || {
                    self.http
                        .post(self.endpoint("/api/v1/user/order"))
                        .json(&payload)
                        .send()
                })
                .await?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("Extended place_order failed with HTTP {status}: {body}");
            }
        }
        Ok(())
    }

    async fn cancel_orders(&self, order_ids: Vec<String>) -> Result<()> {
        for order_id in order_ids {
            let response = self
                .request_policy
                .send_with_retry(&self.rest_governor, "extended_cancel_order", || {
                    let order_id = order_id.clone();
                    let endpoint = self.endpoint("/api/v1/user/order");
                    async move {
                        if order_id.parse::<u64>().is_ok() {
                            self.http
                                .delete(format!("{endpoint}/{order_id}"))
                                .send()
                                .await
                        } else {
                            self.http
                                .delete(endpoint)
                                .query(&[("externalId", order_id.as_str())])
                                .send()
                                .await
                        }
                    }
                })
                .await?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("Extended cancel_order failed with HTTP {status}: {body}");
            }
        }
        Ok(())
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        let payload = json!({ "cancelAll": true });
        let response = self
            .request_policy
            .send_with_retry(&self.rest_governor, "extended_mass_cancel", || {
                self.http
                    .post(self.endpoint("/api/v1/user/order/massCancel"))
                    .json(&payload)
                    .send()
            })
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("Extended massCancel failed with HTTP {status}: {body}");
        }
        Ok(())
    }
}

fn parse_extended_mark_price_events(
    frame: &str,
    symbols: &HashSet<String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(data) = value.get("data") else {
        return Ok(Vec::new());
    };
    let Some(symbol) = data.get("m").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    if !symbols.is_empty() && !symbols.contains(symbol) {
        return Ok(Vec::new());
    }
    let Some(price) = data.get("p").and_then(as_decimal) else {
        return Ok(Vec::new());
    };
    let ts = parse_millis(data.get("ts").and_then(Value::as_i64)).unwrap_or_else(Utc::now);
    Ok(vec![MarketEvent::MarkPrice {
        symbol: symbol.to_string(),
        price,
        timestamp: ts,
    }])
}

fn parse_extended_index_price_events(
    frame: &str,
    symbols: &HashSet<String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(data) = value.get("data") else {
        return Ok(Vec::new());
    };
    let Some(symbol) = data.get("m").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    if !symbols.is_empty() && !symbols.contains(symbol) {
        return Ok(Vec::new());
    }
    let Some(price) = data.get("p").and_then(as_decimal) else {
        return Ok(Vec::new());
    };
    let ts = parse_millis(data.get("ts").and_then(Value::as_i64)).unwrap_or_else(Utc::now);
    Ok(vec![MarketEvent::SpotPrice {
        symbol: symbol.to_string(),
        price,
        timestamp: ts,
    }])
}

fn parse_extended_bbo_events(frame: &str, symbols: &HashSet<String>) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(data) = value.get("data") else {
        return Ok(Vec::new());
    };
    let Some(symbol) = data.get("m").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    if !symbols.is_empty() && !symbols.contains(symbol) {
        return Ok(Vec::new());
    }
    let bid = data
        .get("b")
        .and_then(Value::as_array)
        .and_then(|levels| levels.first())
        .and_then(|level| level.get("p"))
        .and_then(as_decimal);
    let ask = data
        .get("a")
        .and_then(Value::as_array)
        .and_then(|levels| levels.first())
        .and_then(|level| level.get("p"))
        .and_then(as_decimal);
    let (Some(bid), Some(ask)) = (bid, ask) else {
        return Ok(Vec::new());
    };
    let ts = parse_millis(value.get("ts").and_then(Value::as_i64)).unwrap_or_else(Utc::now);
    Ok(vec![MarketEvent::BestBidAsk {
        symbol: symbol.to_string(),
        bid,
        ask,
        bid_size: None,
        ask_size: None,
        timestamp: ts,
    }])
}

fn parse_extended_trade_events(frame: &str, symbols: &HashSet<String>) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(items) = value.get("data").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut events = Vec::new();
    for item in items {
        let Some(symbol) = item.get("m").and_then(Value::as_str) else {
            continue;
        };
        if !symbols.is_empty() && !symbols.contains(symbol) {
            continue;
        }
        let Some(price) = item.get("p").and_then(as_decimal) else {
            continue;
        };
        let Some(quantity) = item.get("q").and_then(as_decimal) else {
            continue;
        };
        let taker_side = match item.get("S").and_then(Value::as_str) {
            Some("BUY") => Some(Side::Bid),
            Some("SELL") => Some(Side::Ask),
            _ => None,
        };
        let ts = parse_millis(item.get("T").and_then(Value::as_i64)).unwrap_or_else(Utc::now);
        events.push(MarketEvent::Trade {
            symbol: symbol.to_string(),
            price,
            quantity,
            taker_side,
            timestamp: ts,
        });
    }
    Ok(events)
}

fn parse_extended_orderbook_events(
    frame: &str,
    symbols: &HashSet<String>,
) -> Result<Vec<MarketEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(data) = value.get("data") else {
        return Ok(Vec::new());
    };
    let Some(symbol) = data.get("m").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    if !symbols.is_empty() && !symbols.contains(symbol) {
        return Ok(Vec::new());
    }
    let bids = parse_extended_levels(data.get("b"));
    let asks = parse_extended_levels(data.get("a"));
    let ts = parse_millis(value.get("ts").and_then(Value::as_i64)).unwrap_or_else(Utc::now);
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("UPDATE");
    let event = if kind.eq_ignore_ascii_case("SNAPSHOT") {
        MarketEvent::OrderBookSnapshot {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: ts,
        }
    } else {
        MarketEvent::OrderBookUpdate {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: ts,
        }
    };
    Ok(vec![event])
}

fn parse_extended_private_events(frame: &str) -> Result<Vec<PrivateEvent>> {
    let value: Value = serde_json::from_str(frame)?;
    let Some(data) = value.get("data") else {
        return Ok(Vec::new());
    };
    let mut events = Vec::new();
    if let Some(balance) = data.get("balance") {
        if let Some(equity) = balance.get("equity").and_then(as_decimal) {
            events.push(PrivateEvent::AccountEquity { equity });
        }
    }
    if let Some(orders) = data.get("orders").and_then(Value::as_array) {
        if value.get("type").and_then(Value::as_str) == Some("ORDER") {
            for order in orders {
                if let Some(parsed) = parse_extended_open_order_value(order) {
                    events.push(PrivateEvent::UpsertOpenOrder(parsed));
                } else if let Some(id) = order.get("id").and_then(value_to_string) {
                    events.push(PrivateEvent::RemoveOpenOrder {
                        order_id: Some(id),
                        nonce: None,
                    });
                }
            }
        } else {
            let snapshot = orders
                .iter()
                .filter_map(parse_extended_open_order_value)
                .collect::<Vec<_>>();
            if !snapshot.is_empty() {
                events.push(PrivateEvent::OpenOrders(snapshot));
            }
        }
    }
    if let Some(positions) = data.get("positions").and_then(Value::as_array) {
        for position in positions {
            if let Ok(parsed) = serde_json::from_value::<ExtendedPosition>(position.clone()) {
                if let Some(position) = parse_extended_position(&parsed) {
                    events.push(PrivateEvent::Position(position));
                }
            }
        }
    }
    if let Some(trades) = data.get("trades").and_then(Value::as_array) {
        for trade in trades {
            if let Some(fill) = parse_extended_fill_value(trade) {
                events.push(PrivateEvent::Fill(fill));
            }
        }
    }
    Ok(events)
}

fn parse_extended_open_order(order: &ExtendedOrder) -> Option<OpenOrder> {
    let price = order.price.as_deref().and_then(|value| value.parse().ok());
    let qty: Decimal = order.qty.parse().ok()?;
    let filled_qty = order
        .filled_qty
        .as_deref()
        .and_then(|value| value.parse().ok())
        .unwrap_or(Decimal::ZERO);
    let remaining_quantity = (qty - filled_qty).max(Decimal::ZERO);
    if remaining_quantity.is_zero() {
        return None;
    }
    Some(OpenOrder {
        order_id: Some(order.id.to_string()),
        nonce: 0,
        level_index: None,
        symbol: order.market.clone(),
        side: parse_side(&order.side)?,
        price,
        remaining_quantity,
    })
}

fn parse_extended_open_order_value(value: &Value) -> Option<OpenOrder> {
    let order: ExtendedOrder = serde_json::from_value(value.clone()).ok()?;
    parse_extended_open_order(&order)
}

fn parse_extended_position(position: &ExtendedPosition) -> Option<Position> {
    let side = parse_position_side(&position.side)?;
    let size: Decimal = position.size.parse().ok()?;
    Some(Position {
        symbol: position.market.clone(),
        quantity: size * side.sign(),
        entry_price: position.open_price.parse().ok()?,
        realized_pnl: position.realised_pnl.parse().ok().unwrap_or(Decimal::ZERO),
        unrealized_pnl: position
            .unrealised_pnl
            .parse()
            .ok()
            .unwrap_or(Decimal::ZERO),
        // Only authoritative when the venue actually sends a non-zero realized_pnl.
        pnl_is_authoritative: position
            .realised_pnl
            .parse::<Decimal>()
            .ok()
            .map(|pnl| !pnl.is_zero())
            .unwrap_or(false),
        opened_at: None,
    })
}

fn parse_extended_fill_value(value: &Value) -> Option<Fill> {
    let symbol = value
        .get("market")
        .or_else(|| value.get("m"))?
        .as_str()?
        .to_string();
    let side = value
        .get("side")
        .or_else(|| value.get("S"))
        .and_then(Value::as_str)
        .and_then(parse_side)?;
    let price = value
        .get("price")
        .or_else(|| value.get("p"))
        .and_then(as_decimal)?;
    let quantity = value
        .get("qty")
        .or_else(|| value.get("q"))
        .and_then(as_decimal)?;
    let timestamp = parse_millis(
        value
            .get("createdTime")
            .or_else(|| value.get("updatedTime"))
            .or_else(|| value.get("T"))
            .and_then(Value::as_i64),
    )
    .unwrap_or_else(Utc::now);
    Some(Fill {
        order_id: value
            .get("id")
            .or_else(|| value.get("orderId"))
            .and_then(|id| {
                id.as_str()
                    .map(ToString::to_string)
                    .or_else(|| id.as_u64().map(|v| v.to_string()))
            }),
        nonce: value.get("nonce").and_then(|nonce| {
            nonce
                .as_u64()
                .or_else(|| nonce.as_str()?.parse::<u64>().ok())
        }),
        symbol,
        side,
        price,
        quantity,
        fee_paid: value
            .get("fee")
            .or_else(|| value.get("fees"))
            .or_else(|| value.get("payedFee"))
            .or_else(|| value.get("commission"))
            .or_else(|| value.get("f"))
            .and_then(as_decimal),
        funding_paid: value
            .get("funding")
            .or_else(|| value.get("fundingPaid"))
            .or_else(|| value.get("funding_payment"))
            .and_then(as_decimal),
        timestamp,
    })
}

fn parse_extended_levels(value: Option<&Value>) -> BTreeMap<Decimal, Decimal> {
    let mut levels = BTreeMap::new();
    let Some(items) = value.and_then(Value::as_array) else {
        return levels;
    };
    for item in items {
        let Some(price) = item.get("p").and_then(as_decimal) else {
            continue;
        };
        let quantity = item.get("q").and_then(as_decimal).unwrap_or(Decimal::ZERO);
        levels.insert(price, quantity);
    }
    levels
}

fn parse_side(value: &str) -> Option<Side> {
    match value {
        "BUY" => Some(Side::Bid),
        "SELL" => Some(Side::Ask),
        _ => None,
    }
}

fn parse_position_side(value: &str) -> Option<Side> {
    match value {
        "LONG" => Some(Side::Bid),
        "SHORT" => Some(Side::Ask),
        _ => None,
    }
}

fn parse_millis(value: Option<i64>) -> Option<chrono::DateTime<Utc>> {
    Utc.timestamp_millis_opt(value?).single()
}

fn as_decimal(value: &Value) -> Option<Decimal> {
    value_to_string(value)?.parse().ok()
}

fn value_to_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .or_else(|| value.as_i64().map(|v| v.to_string()))
        .or_else(|| value.as_u64().map(|v| v.to_string()))
}

fn parse_decimal_string(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw).with_context(|| format!("invalid decimal: {raw}"))
}

fn format_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

fn round_stark_amount(
    amount: Decimal,
    resolution: u64,
    strategy: RoundingStrategy,
) -> Result<i128> {
    let scaled = amount * Decimal::from(resolution);
    scaled
        .round_dp_with_strategy(0, strategy)
        .to_i128()
        .with_context(|| format!("unable to convert scaled amount {scaled} to integer"))
}

fn quantize_down_to_step(value: Decimal, step: Decimal) -> Result<Decimal> {
    if step <= Decimal::ZERO {
        bail!("Extended quantity step must be positive, got {step}");
    }
    Ok(((value / step).floor() * step).normalize())
}

fn ceil_timestamp_millis_to_seconds(timestamp_millis: i64) -> i64 {
    let seconds = timestamp_millis.div_euclid(1000);
    let remainder = timestamp_millis.rem_euclid(1000);
    if remainder == 0 {
        seconds
    } else {
        seconds + 1
    }
}

fn generate_extended_nonce() -> i64 {
    let nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    nanos.rem_euclid(i64::from(i32::MAX - 1)) + 1
}

fn build_extended_external_id(request: &OrderRequest) -> String {
    format!(
        "{}-{}-{}-{}",
        request.symbol,
        match request.side {
            Side::Bid => "buy",
            Side::Ask => "sell",
        },
        request.level_index,
        Utc::now().timestamp_millis()
    )
}

fn extended_starknet_domain(api_base_url: &str) -> StarknetDomain {
    let is_sepolia = api_base_url.to_ascii_lowercase().contains("sepolia");
    StarknetDomain {
        name: "Perpetuals".to_string(),
        version: "v0".to_string(),
        chain_id: if is_sepolia {
            "SN_SEPOLIA".to_string()
        } else {
            "SN_MAIN".to_string()
        },
        revision: 1,
    }
}

fn decimals_from_resolution(resolution: u64) -> u32 {
    if resolution == 0 {
        return 0;
    }
    let mut digits = 0u32;
    let mut n = resolution;
    while n > 1 && n % 10 == 0 {
        n /= 10;
        digits += 1;
    }
    digits
}

#[derive(Debug, Deserialize)]
struct ExtendedResponse<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedFundingHistoryResponse {
    data: Vec<ExtendedFundingPayment>,
    pagination: Option<ExtendedCursorPagination>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedFundingPayment {
    id: u64,
    account_id: u64,
    market: String,
    position_id: u64,
    side: String,
    size: String,
    value: String,
    mark_price: String,
    funding_fee: String,
    funding_rate: String,
    paid_time: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedCursorPagination {
    #[serde(default, deserialize_with = "deserialize_optional_string_or_number")]
    cursor: Option<String>,
}

#[derive(Clone, Debug)]
struct ExtendedAccountIdentity {
    vault: u64,
    public_key_hex: String,
    public_key: Felt,
    private_key: Felt,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedAccountDetails {
    l2_key: String,
    #[serde(deserialize_with = "deserialize_u64_from_string_or_number")]
    l2_vault: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedMarket {
    #[serde(alias = "name")]
    market: String,
    market_stats: ExtendedMarketStats,
    trading_config: ExtendedTradingConfig,
    l2_config: ExtendedL2Config,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedMarketStats {
    bid_price: String,
    ask_price: String,
    mark_price: String,
    index_price: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedTradingConfig {
    min_order_size: String,
    min_order_size_change: String,
    min_price_change: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedL2Config {
    collateral_id: String,
    collateral_resolution: u64,
    synthetic_id: String,
    synthetic_resolution: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedBalanceSnapshot {
    equity: String,
}

fn deserialize_u64_from_string_or_number<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected unsigned integer")),
        Value::String(text) => text
            .parse::<u64>()
            .map_err(|err| serde::de::Error::custom(err.to_string())),
        other => Err(serde::de::Error::custom(format!(
            "expected string or number for u64, got {other}"
        ))),
    }
}

fn deserialize_optional_string_or_number<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(Value::Number(number)) => Ok(Some(number.to_string())),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected optional string or number, got {other}"
        ))),
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedPosition {
    market: String,
    side: String,
    size: String,
    open_price: String,
    unrealised_pnl: String,
    realised_pnl: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedOrder {
    id: u64,
    market: String,
    side: String,
    price: Option<String>,
    qty: String,
    filled_qty: Option<String>,
}
