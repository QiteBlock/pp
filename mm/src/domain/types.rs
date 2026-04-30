use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstrumentMeta {
    pub symbol: String,
    pub contract_id: u32,
    pub underlying_decimals: u32,
    pub settlement_decimals: u32,
    pub min_order_size: Decimal,
    pub tick_size: Option<Decimal>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Ask,
    Bid,
}

impl Side {
    pub fn sign(self) -> Decimal {
        match self {
            Side::Bid => Decimal::ONE,
            Side::Ask => -Decimal::ONE,
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

impl Default for Side {
    fn default() -> Self {
        Self::Bid
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeInForce {
    #[default]
    GoodTillTime,
    ImmediateOrCancel,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketRegime {
    #[default]
    Quiet,
    VolatileBalanced,
    TrendingToxic,
}

#[derive(Clone, Debug)]
pub struct QuoteIntent {
    pub symbol: String,
    pub level_index: usize,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub post_only: bool,
}

#[derive(Clone, Debug)]
pub struct OrderRequest {
    pub symbol: String,
    pub contract_id: u32,
    pub level_index: usize,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub post_only: bool,
}

#[derive(Clone, Debug, Default)]
pub struct OpenOrder {
    pub order_id: Option<String>,
    pub nonce: u64,
    pub level_index: Option<usize>,
    pub symbol: String,
    pub side: Side,
    pub price: Option<Decimal>,
    pub remaining_quantity: Decimal,
}

#[derive(Clone, Debug)]
pub struct Fill {
    pub order_id: Option<String>,
    pub nonce: Option<u64>,
    pub symbol: String,
    pub side: Side,
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct Position {
    pub symbol: String,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub pnl_is_authoritative: bool,
    pub opened_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub enum MarketEvent {
    StreamReconnected {
        symbols: Vec<String>,
    },
    MarkPrice {
        symbol: String,
        price: Decimal,
        timestamp: DateTime<Utc>,
    },
    SpotPrice {
        symbol: String,
        price: Decimal,
        timestamp: DateTime<Utc>,
    },
    BestBidAsk {
        symbol: String,
        bid: Decimal,
        ask: Decimal,
        /// BBO queue sizes from the exchange (e.g. GRVT v1.mini.d `bq`/`aq`).
        /// None when the venue doesn't publish BBO depths.
        bid_size: Option<Decimal>,
        ask_size: Option<Decimal>,
        timestamp: DateTime<Utc>,
    },
    FundingRate {
        symbol: String,
        /// Signed periodic funding rate (positive = longs pay shorts).
        rate: Decimal,
        timestamp: DateTime<Utc>,
    },
    Trade {
        symbol: String,
        price: Decimal,
        quantity: Decimal,
        taker_side: Option<Side>,
        timestamp: DateTime<Utc>,
    },
    OrderBookSnapshot {
        symbol: String,
        bids: BTreeMap<Decimal, Decimal>,
        asks: BTreeMap<Decimal, Decimal>,
        timestamp: DateTime<Utc>,
    },
    OrderBookUpdate {
        symbol: String,
        bids: BTreeMap<Decimal, Decimal>,
        asks: BTreeMap<Decimal, Decimal>,
        timestamp: DateTime<Utc>,
    },
}

#[derive(Clone, Debug)]
pub enum PrivateEvent {
    StreamReconnected,
    OpenOrders(Vec<OpenOrder>),
    UpsertOpenOrder(OpenOrder),
    RemoveOpenOrder {
        order_id: Option<String>,
        nonce: Option<u64>,
    },
    Fill(Fill),
    Position(Position),
    AccountEquity {
        equity: Decimal,
    },
}

#[derive(Clone, Debug)]
pub struct FactorSnapshot {
    pub price_index: Decimal,
    pub raw_volatility: Decimal,
    pub volatility: Decimal,
    /// Per-pair inventory lean in bps, scaled from the pair's BBO spread.
    pub inventory_lean_bps: Decimal,
    /// Pure volume-based size scalar, always >= 0. Use for order sizing. (S6)
    pub volume_imbalance: Decimal,
    /// Signed flow direction [-1, 1]: positive = buy pressure, negative = sell pressure. (S6)
    pub flow_direction: Decimal,
    pub inventory_skew: Decimal,
    pub recent_trade_count: usize,
    pub regime: MarketRegime,
    /// Continuous regime intensity in [0, 1]: 0 = fully Quiet, 1 = fully TrendingToxic. (S5)
    pub regime_intensity: Decimal,
    /// Orderbook imbalance [-1, 1]: positive = bid-heavy (upward pressure), negative = ask-heavy.
    /// Leading signal computed from top-N levels of the live orderbook.
    pub ob_imbalance: Decimal,
    /// Consecutive factor cycles where |flow_direction| exceeded flow_spike_pause_threshold.
    /// The engine requires ≥ 2 before suppressing quotes to prevent oscillating-flow false positives.
    pub consecutive_flow_spike: u32,
    /// BBO size-weighted mid (microprice). Falls back to raw mid when BBO sizes are unavailable.
    /// Used as the primary fair-value estimate in distribution.
    pub microprice: Decimal,
    /// Signed fill-rate skew [-1, 1]: positive = bid fills faster, negative = ask fills faster.
    /// Derived from a rolling window of bid vs ask fill counts.
    pub fill_rate_skew: Decimal,
    /// VPIN ∈ [0, 1]: high values indicate informed trading / adverse selection risk.
    pub vpin: Decimal,
    /// Funding-rate lean in bps: positive shifts reservation ask-ward (collect long funding),
    /// negative shifts it bid-ward. Zero when funding data is absent or weight = 0.
    pub funding_lean: Decimal,
    /// Online kappa estimate from empirical fill probability at level 0.
    /// None until enough cycles have been observed (controlled by `kappa_min_cycles`).
    pub kappa_estimate: Option<Decimal>,
    /// Post-fill spread widen multiplier for the bid side (1.0 = normal, > 1.0 = wider).
    /// Populated by the engine after a recent ask fill triggers the post-fill window.
    pub post_fill_widen_bid: Decimal,
    /// Post-fill spread widen multiplier for the ask side.
    pub post_fill_widen_ask: Decimal,
    /// Extra spread widening applied during a sustained flow spike.
    pub flow_spike_widen_multiplier: Decimal,
    /// Seconds spent continuously in the current TrendingToxic regime.
    pub toxic_regime_persistence_secs: u64,
}

impl Default for FactorSnapshot {
    fn default() -> Self {
        Self {
            price_index: Decimal::ZERO,
            raw_volatility: Decimal::ZERO,
            volatility: Decimal::ZERO,
            inventory_lean_bps: Decimal::ZERO,
            volume_imbalance: Decimal::ZERO,
            flow_direction: Decimal::ZERO,
            inventory_skew: Decimal::ZERO,
            recent_trade_count: 0,
            regime: MarketRegime::default(),
            regime_intensity: Decimal::ZERO,
            ob_imbalance: Decimal::ZERO,
            consecutive_flow_spike: 0,
            microprice: Decimal::ZERO,
            fill_rate_skew: Decimal::ZERO,
            vpin: Decimal::ZERO,
            funding_lean: Decimal::ZERO,
            kappa_estimate: None,
            post_fill_widen_bid: Decimal::ONE,
            post_fill_widen_ask: Decimal::ONE,
            flow_spike_widen_multiplier: Decimal::ONE,
            toxic_regime_persistence_secs: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StrategySnapshot {
    pub ts: DateTime<Utc>,
    pub symbol: String,
    pub is_simulated: bool,
    pub decision: String,
    pub skip_reason: Option<String>,
    pub current_position: Decimal,
    pub position_price: Decimal,
    pub position_notional: Decimal,
    pub has_position: bool,
    pub unwind_only: bool,
    pub stale_maker_unwind: bool,
    pub emergency_unwind: bool,
    pub degraded: bool,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub mid_price: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub spot_price: Option<Decimal>,
    pub bbo_spread_bps: Option<Decimal>,
    pub bbo_bid_size: Option<Decimal>,
    pub bbo_ask_size: Option<Decimal>,
    pub price_index: Option<Decimal>,
    pub raw_volatility: Option<Decimal>,
    pub volatility: Option<Decimal>,
    pub inventory_lean_bps: Option<Decimal>,
    pub volume_imbalance: Option<Decimal>,
    pub flow_direction: Option<Decimal>,
    pub inventory_skew: Option<Decimal>,
    pub recent_trade_count: Option<i64>,
    pub regime: Option<String>,
    pub regime_intensity: Option<Decimal>,
    pub ob_imbalance: Option<Decimal>,
    pub consecutive_flow_spike: Option<i64>,
    pub microprice: Option<Decimal>,
    pub fill_rate_skew: Option<Decimal>,
    pub vpin: Option<Decimal>,
    pub funding_lean: Option<Decimal>,
    pub kappa_estimate: Option<Decimal>,
    pub post_fill_widen_bid: Option<Decimal>,
    pub post_fill_widen_ask: Option<Decimal>,
    pub flow_spike_widen_multiplier: Option<Decimal>,
    pub toxic_regime_persistence_secs: Option<i64>,
    pub bid_lvl0_multiplier: Option<Decimal>,
    pub ask_lvl0_multiplier: Option<Decimal>,
    pub generated_quote_count: i64,
    pub desired_order_count: i64,
    pub bid_count: i64,
    pub ask_count: i64,
    pub total_bid_qty: Decimal,
    pub total_ask_qty: Decimal,
    pub top_bid: Option<Decimal>,
    pub top_ask: Option<Decimal>,
    pub account_equity: Decimal,
    pub total_pnl: Decimal,
}

#[derive(Clone, Debug)]
pub struct QuoteFilterEvent {
    pub ts: DateTime<Utc>,
    pub symbol: String,
    pub is_simulated: bool,
    pub reason: String,
    pub side: Side,
    pub level_index: i64,
    pub is_unwind: bool,
    pub unwind_only: bool,
    pub stale_maker_unwind: bool,
    pub emergency_unwind: bool,
    pub current_position: Decimal,
    pub raw_quantity: Decimal,
    pub effective_quantity: Option<Decimal>,
    pub remaining_capacity: Option<Decimal>,
    pub price: Option<Decimal>,
    pub notional: Option<Decimal>,
    pub quote_min_trade_amount: Option<Decimal>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
}

#[derive(Clone, Debug)]
pub struct RecentTrade {
    pub quantity: Decimal,
    pub timestamp: DateTime<Utc>,
    pub taker_side: Option<Side>,
}
