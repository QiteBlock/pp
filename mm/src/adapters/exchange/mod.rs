use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::config::ExchangeKind;
use crate::domain::{
    FundingPaymentEvent, InstrumentMeta, MarketEvent, OpenOrder, OrderRequest, Position,
    PrivateEvent,
};

use self::{extended::ExtendedClient, grvt::GrvtClient, hibachi::HibachiClient};

pub mod extended;
pub mod grvt;
pub mod hibachi;

#[async_trait]
pub trait MarketDataSource: Send + Sync {
    async fn stream_mark_prices(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()>;

    async fn stream_spot_prices(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()>;

    async fn stream_best_bid_ask(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()>;

    async fn stream_trades(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()>;

    async fn stream_orderbook(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()>;
}

#[async_trait]
pub trait PrivateDataSource: Send + Sync {
    async fn stream_private_data(
        &self,
        sender: tokio::sync::mpsc::Sender<PrivateEvent>,
    ) -> Result<()>;

    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>>;
    async fn fetch_positions(&self) -> Result<Vec<Position>>;
}

#[async_trait]
pub trait OrderExecutor: Send + Sync {
    async fn place_orders(&self, requests: Vec<OrderRequest>) -> Result<()>;
    async fn cancel_orders(&self, order_ids: Vec<String>) -> Result<()>;
    async fn cancel_all_orders(&self) -> Result<()>;
}

#[async_trait]
pub trait ExchangeClient:
    MarketDataSource + PrivateDataSource + OrderExecutor + Send + Sync
{
    async fn load_instruments(&self) -> Result<Vec<InstrumentMeta>>;

    /// Optional REST fallback: fetch current BBO / mark price for the given symbols.
    /// Returns an empty vec for venues that do not support this (default).
    async fn fetch_market_snapshot(&self, _symbols: &[String]) -> Vec<MarketEvent> {
        Vec::new()
    }

    /// Optional REST sync: fetch account-level funding payment history since the given time.
    async fn fetch_funding_payments(
        &self,
        _symbols: &[String],
        _start_time: DateTime<Utc>,
    ) -> Result<Vec<FundingPaymentEvent>> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
pub enum AnyExchangeClient {
    Hibachi(HibachiClient),
    Grvt(GrvtClient),
    Extended(ExtendedClient),
}

impl AnyExchangeClient {
    pub fn kind(&self) -> ExchangeKind {
        match self {
            Self::Hibachi(_) => ExchangeKind::Hibachi,
            Self::Grvt(_) => ExchangeKind::Grvt,
            Self::Extended(_) => ExchangeKind::Extended,
        }
    }
}

#[async_trait]
impl MarketDataSource for AnyExchangeClient {
    async fn stream_mark_prices(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.stream_mark_prices(symbols, sender).await,
            Self::Grvt(client) => client.stream_mark_prices(symbols, sender).await,
            Self::Extended(client) => client.stream_mark_prices(symbols, sender).await,
        }
    }

    async fn stream_spot_prices(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.stream_spot_prices(symbols, sender).await,
            Self::Grvt(client) => client.stream_spot_prices(symbols, sender).await,
            Self::Extended(client) => client.stream_spot_prices(symbols, sender).await,
        }
    }

    async fn stream_best_bid_ask(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.stream_best_bid_ask(symbols, sender).await,
            Self::Grvt(client) => client.stream_best_bid_ask(symbols, sender).await,
            Self::Extended(client) => client.stream_best_bid_ask(symbols, sender).await,
        }
    }

    async fn stream_trades(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.stream_trades(symbols, sender).await,
            Self::Grvt(client) => client.stream_trades(symbols, sender).await,
            Self::Extended(client) => client.stream_trades(symbols, sender).await,
        }
    }

    async fn stream_orderbook(
        &self,
        symbols: &[String],
        sender: tokio::sync::mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.stream_orderbook(symbols, sender).await,
            Self::Grvt(client) => client.stream_orderbook(symbols, sender).await,
            Self::Extended(client) => client.stream_orderbook(symbols, sender).await,
        }
    }
}

#[async_trait]
impl PrivateDataSource for AnyExchangeClient {
    async fn stream_private_data(
        &self,
        sender: tokio::sync::mpsc::Sender<PrivateEvent>,
    ) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.stream_private_data(sender).await,
            Self::Grvt(client) => client.stream_private_data(sender).await,
            Self::Extended(client) => client.stream_private_data(sender).await,
        }
    }

    async fn fetch_open_orders(&self) -> Result<Vec<OpenOrder>> {
        match self {
            Self::Hibachi(client) => client.fetch_open_orders().await,
            Self::Grvt(client) => client.fetch_open_orders().await,
            Self::Extended(client) => client.fetch_open_orders().await,
        }
    }

    async fn fetch_positions(&self) -> Result<Vec<Position>> {
        match self {
            Self::Hibachi(client) => client.fetch_positions().await,
            Self::Grvt(client) => client.fetch_positions().await,
            Self::Extended(client) => client.fetch_positions().await,
        }
    }
}

#[async_trait]
impl OrderExecutor for AnyExchangeClient {
    async fn place_orders(&self, requests: Vec<OrderRequest>) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.place_orders(requests).await,
            Self::Grvt(client) => client.place_orders(requests).await,
            Self::Extended(client) => client.place_orders(requests).await,
        }
    }

    async fn cancel_orders(&self, order_ids: Vec<String>) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.cancel_orders(order_ids).await,
            Self::Grvt(client) => client.cancel_orders(order_ids).await,
            Self::Extended(client) => client.cancel_orders(order_ids).await,
        }
    }

    async fn cancel_all_orders(&self) -> Result<()> {
        match self {
            Self::Hibachi(client) => client.cancel_all_orders().await,
            Self::Grvt(client) => client.cancel_all_orders().await,
            Self::Extended(client) => client.cancel_all_orders().await,
        }
    }
}

#[async_trait]
impl ExchangeClient for AnyExchangeClient {
    async fn load_instruments(&self) -> Result<Vec<InstrumentMeta>> {
        match self {
            Self::Hibachi(client) => client.load_instruments().await,
            Self::Grvt(client) => client.load_instruments().await,
            Self::Extended(client) => client.load_instruments().await,
        }
    }

    async fn fetch_market_snapshot(&self, symbols: &[String]) -> Vec<MarketEvent> {
        match self {
            Self::Hibachi(_) => Vec::new(),
            Self::Grvt(client) => client.fetch_market_snapshot(symbols).await,
            Self::Extended(client) => client.fetch_market_snapshot(symbols).await,
        }
    }

    async fn fetch_funding_payments(
        &self,
        symbols: &[String],
        start_time: DateTime<Utc>,
    ) -> Result<Vec<FundingPaymentEvent>> {
        match self {
            Self::Hibachi(_) => Ok(Vec::new()),
            Self::Grvt(client) => client.fetch_funding_payments(symbols, start_time).await,
            Self::Extended(client) => client.fetch_funding_payments(symbols, start_time).await,
        }
    }
}
