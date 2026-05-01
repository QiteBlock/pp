use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rust_decimal::prelude::Signed;
use rust_decimal::Decimal;
use tracing::warn;

use crate::domain::{Fill, MarketEvent, OpenOrder, Position, PrivateEvent, RecentTrade};

pub const MAX_RECENT_TRADES: usize = 10_000;
const MAX_FILL_HISTORY: usize = 10_000;

/// How long after a stream reconnect to ignore position-snapshot WS updates.
/// Fills are processed first (they arrive before position snapshots); the settle
/// window prevents a stale position snapshot from overwriting the fill-derived qty.
const POSITION_SETTLE_AFTER_RECONNECT: Duration = Duration::from_secs(5);

pub struct BotState {
    pub market: MarketState,
    pub open_orders: HashMap<String, OpenOrder>,
    pub dry_run_orders: HashMap<String, OpenOrder>,
    pub positions: HashMap<String, Position>,
    pub fills: VecDeque<Fill>,
    pub pnl: PnlState,
    pub account_equity: Decimal,
    pub startup_account_equity: Option<Decimal>,
    maker_fee_rate: Decimal,
    /// Set to `Some(deadline)` on stream reconnect; position WS updates are
    /// ignored until the deadline to let fill-replay settle first.
    position_settle_until: Option<Instant>,
}

impl BotState {
    pub fn new(maker_fee_rate: Decimal, max_orderbook_depth: usize) -> Self {
        Self {
            market: MarketState::new(max_orderbook_depth),
            open_orders: HashMap::new(),
            dry_run_orders: HashMap::new(),
            positions: HashMap::new(),
            fills: VecDeque::new(),
            pnl: PnlState::default(),
            account_equity: Decimal::ZERO,
            startup_account_equity: None,
            maker_fee_rate,
            position_settle_until: None,
        }
    }

    pub fn apply_market_event(&mut self, event: MarketEvent) {
        self.market.apply(event);
        self.refresh_unrealized_pnl();
    }

    pub fn apply_private_event(&mut self, event: PrivateEvent) {
        match event {
            PrivateEvent::StreamReconnected => {
                self.open_orders.clear();
                self.position_settle_until = Some(Instant::now() + POSITION_SETTLE_AFTER_RECONNECT);
            }
            PrivateEvent::OpenOrders(orders) => {
                self.open_orders.clear();
                for order in orders {
                    let order_key = order
                        .order_id
                        .clone()
                        .unwrap_or_else(|| format!("nonce:{}", order.nonce));
                    self.open_orders.insert(order_key, order);
                }
            }
            PrivateEvent::UpsertOpenOrder(order) => {
                let order_key = order
                    .order_id
                    .clone()
                    .unwrap_or_else(|| format!("nonce:{}", order.nonce));
                self.open_orders.insert(order_key, order);
            }
            PrivateEvent::RemoveOpenOrder { order_id, nonce } => {
                if let Some(order_id) = order_id {
                    self.open_orders.remove(&order_id);
                } else if let Some(nonce) = nonce {
                    self.open_orders.remove(&format!("nonce:{nonce}"));
                }
            }
            PrivateEvent::Fill(fill) => self.apply_fill(fill),
            PrivateEvent::Position(position) => self.apply_position_update(position),
            PrivateEvent::AccountEquity { equity } => self.apply_account_equity(equity),
        }

        self.refresh_unrealized_pnl();
    }

    pub fn total_abs_position_notional(&self) -> Decimal {
        self.positions
            .values()
            .map(|position| {
                let mark = self
                    .market
                    .symbols
                    .get(&position.symbol)
                    .and_then(|state| state.mark_price.or_else(|| state.mid_price()))
                    .unwrap_or(Decimal::ZERO);
                (position.quantity * mark).abs()
            })
            .sum()
    }

    pub fn session_account_pnl(&self) -> Option<Decimal> {
        self.startup_account_equity
            .map(|baseline| self.account_equity - baseline)
    }

    pub fn effective_total_pnl(&self) -> Decimal {
        self.session_account_pnl().unwrap_or(self.pnl.total_pnl)
    }

    pub fn apply_dry_run_plan(
        &mut self,
        to_cancel_order_ids: &[String],
        to_place: &[crate::domain::OrderRequest],
    ) {
        for order_id in to_cancel_order_ids {
            self.dry_run_orders.remove(order_id);
        }

        for order in to_place {
            let order_id = dry_run_order_id(&order.symbol, order.side, order.level_index);
            self.dry_run_orders.insert(
                order_id.clone(),
                OpenOrder {
                    order_id: Some(order_id),
                    nonce: 0,
                    level_index: Some(order.level_index),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    price: order.price,
                    remaining_quantity: order.quantity,
                },
            );
        }
    }

    pub fn simulate_dry_run_trade(
        &mut self,
        symbol: &str,
        price: Decimal,
        quantity: Decimal,
        timestamp: DateTime<Utc>,
    ) -> Vec<Fill> {
        if quantity <= Decimal::ZERO {
            return Vec::new();
        }

        let mut matched_orders: Vec<OpenOrder> = self
            .dry_run_orders
            .values()
            .filter(|order| order.symbol == symbol)
            .filter(|order| match (order.side, order.price) {
                (crate::domain::Side::Bid, Some(order_price)) => price <= order_price,
                (crate::domain::Side::Ask, Some(order_price)) => price >= order_price,
                _ => false,
            })
            .cloned()
            .collect();

        matched_orders.sort_by(|left, right| {
            let left_price = left.price.unwrap_or(Decimal::ZERO);
            let right_price = right.price.unwrap_or(Decimal::ZERO);
            match left.side {
                crate::domain::Side::Bid => right_price.cmp(&left_price),
                crate::domain::Side::Ask => left_price.cmp(&right_price),
            }
        });

        let mut remaining_trade_quantity = quantity;
        let mut fills = Vec::new();

        for matched in matched_orders {
            if remaining_trade_quantity <= Decimal::ZERO {
                break;
            }

            let fill_quantity = matched.remaining_quantity.min(remaining_trade_quantity);
            if fill_quantity <= Decimal::ZERO {
                continue;
            }

            let Some(order_id) = matched.order_id.clone() else {
                continue;
            };

            let mut remove_order = false;
            if let Some(order) = self.dry_run_orders.get_mut(&order_id) {
                order.remaining_quantity -= fill_quantity;
                remove_order = order.remaining_quantity <= Decimal::ZERO;
            }
            if remove_order {
                self.dry_run_orders.remove(&order_id);
            }

            remaining_trade_quantity -= fill_quantity;
            fills.push(Fill {
                order_id: matched.order_id.clone(),
                nonce: Some(matched.nonce),
                symbol: matched.symbol.clone(),
                side: matched.side,
                price: matched.price.unwrap_or(price),
                quantity: fill_quantity,
                fee_paid: None,
                funding_paid: None,
                timestamp,
            });
        }

        fills
    }

    pub fn refresh_after_external_position_reset(&mut self) {
        self.refresh_unrealized_pnl();
    }

    fn apply_account_equity(&mut self, equity: Decimal) {
        self.account_equity = equity;
        if self.startup_account_equity.is_none() {
            self.startup_account_equity = Some(equity);
        }
    }

    fn refresh_unrealized_pnl(&mut self) {
        let mut total = -self.pnl.total_fees;
        for position in self.positions.values_mut() {
            total += position.realized_pnl;
            if position.pnl_is_authoritative {
                total += position.unrealized_pnl;
                continue;
            }
            let Some(market) = self.market.symbols.get(&position.symbol) else {
                position.unrealized_pnl = Decimal::ZERO;
                continue;
            };
            let Some(mark) = market.mark_price.or_else(|| market.mid_price()) else {
                position.unrealized_pnl = Decimal::ZERO;
                continue;
            };
            position.unrealized_pnl = (mark - position.entry_price) * position.quantity;
            total += position.unrealized_pnl;
        }
        self.pnl.total_pnl = total;
    }

    fn apply_position_update(&mut self, position: Position) {
        // Ignore position snapshots during the settle window after a reconnect.
        // Fills are replayed first; this prevents a stale snapshot from
        // overwriting the fill-derived quantity.
        if self
            .position_settle_until
            .map(|deadline| Instant::now() < deadline)
            .unwrap_or(false)
        {
            warn!(
                symbol = %position.symbol,
                "ignoring position WS update during reconnect settle window"
            );
            return;
        }

        let existing = self.positions.get(&position.symbol).cloned();

        // Delta sanity check: skip implausibly large jumps that look like
        // stale position snapshots arriving after fills have already been applied.
        if let Some(ref existing) = existing {
            let delta = (position.quantity - existing.quantity).abs();
            let max_recent_fill = self
                .fills
                .iter()
                .map(|f| f.quantity.abs())
                .fold(Decimal::ZERO, |acc, q| acc.max(q));
            if !max_recent_fill.is_zero() && delta > max_recent_fill * Decimal::from(2u64) {
                warn!(
                    symbol = %position.symbol,
                    delta = %delta,
                    max_recent_fill = %max_recent_fill,
                    "position delta sanity check failed; skipping stale position update"
                );
                return;
            }
        }

        let mut merged = position.clone();

        if let Some(ref existing) = existing {
            if merged.entry_price.is_zero() && !merged.quantity.is_zero() {
                merged.entry_price = existing.entry_price;
            }
            if !merged.pnl_is_authoritative {
                merged.unrealized_pnl = existing.unrealized_pnl;
            }
            // GRVT position WS messages often omit realized_pnl or send zero.
            // Preserve the running fill-based total unless the venue explicitly
            // provided an authoritative realized P&L value for this update.
            if !merged.pnl_is_authoritative && !existing.realized_pnl.is_zero() {
                merged.realized_pnl = existing.realized_pnl;
            }
            if !existing.quantity.is_zero()
                && !merged.quantity.is_zero()
                && merged.quantity.signum() != existing.quantity.signum()
            {
                merged.opened_at = Some(Utc::now());
            } else if !existing.quantity.is_zero() && !merged.quantity.is_zero() {
                merged.opened_at = existing.opened_at;
            } else if existing.quantity.is_zero() && !merged.quantity.is_zero() {
                merged.opened_at = Some(Utc::now());
            }
        } else if !merged.quantity.is_zero() {
            merged.opened_at = Some(Utc::now());
        }

        if merged.quantity.is_zero() {
            merged.opened_at = None;
        }

        self.positions.insert(merged.symbol.clone(), merged);
    }

    fn apply_fill(&mut self, fill: Fill) {
        let position = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert_with(|| Position {
                symbol: fill.symbol.clone(),
                ..Position::default()
            });

        let fill_quantity = fill.quantity * fill.side.sign();
        let previous_quantity = position.quantity;
        let new_quantity = previous_quantity + fill_quantity;

        if previous_quantity.is_zero() || previous_quantity.signum() == fill_quantity.signum() {
            let previous_abs = previous_quantity.abs();
            let fill_abs = fill_quantity.abs();
            let total_abs = previous_abs + fill_abs;
            if total_abs.is_zero() {
                position.entry_price = Decimal::ZERO;
            } else {
                position.entry_price =
                    ((position.entry_price * previous_abs) + (fill.price * fill_abs)) / total_abs;
            }
        } else {
            let closed_quantity = previous_quantity.abs().min(fill_quantity.abs());
            if !position.pnl_is_authoritative {
                position.realized_pnl += (fill.price - position.entry_price)
                    * closed_quantity
                    * previous_quantity.signum();
            }

            if new_quantity.is_zero() {
                position.entry_price = Decimal::ZERO;
            } else if new_quantity.signum() != previous_quantity.signum() {
                position.entry_price = fill.price;
            }
        }

        position.quantity = new_quantity;
        if previous_quantity.is_zero() && !new_quantity.is_zero() {
            position.opened_at = Some(fill.timestamp);
        } else if new_quantity.is_zero() {
            position.opened_at = None;
        } else if !previous_quantity.is_zero()
            && new_quantity.signum() != previous_quantity.signum()
        {
            position.opened_at = Some(fill.timestamp);
        }
        let estimated_fee = (fill.price * fill.quantity).abs() * self.maker_fee_rate;
        self.pnl.total_fees += estimated_fee;
        self.fills.push_back(fill);
        while self.fills.len() > MAX_FILL_HISTORY {
            self.fills.pop_front();
        }
    }
}

fn dry_run_order_id(symbol: &str, side: crate::domain::Side, level_index: usize) -> String {
    format!("dryrun:{symbol}:{side:?}:{level_index}")
}

#[derive(Default)]
pub struct PnlState {
    pub total_pnl: Decimal,
    pub total_fees: Decimal,
}

pub struct MarketState {
    pub symbols: HashMap<String, SymbolMarketState>,
    pub last_updated: Option<DateTime<Utc>>,
    pub last_updated_at: Option<Instant>,
    max_orderbook_depth: usize,
}

impl MarketState {
    pub fn new(max_orderbook_depth: usize) -> Self {
        Self {
            symbols: HashMap::new(),
            last_updated: None,
            last_updated_at: None,
            max_orderbook_depth,
        }
    }

    pub fn apply(&mut self, event: MarketEvent) {
        match event {
            MarketEvent::StreamReconnected { symbols } => {
                for symbol in symbols {
                    if let Some(symbol_state) = self.symbols.get_mut(&symbol) {
                        symbol_state.best_bid = None;
                        symbol_state.best_ask = None;
                        symbol_state.bbo_bid_size = None;
                        symbol_state.bbo_ask_size = None;
                        symbol_state.mark_price = None;
                        symbol_state.spot_price = None;
                        symbol_state.last_trade_price = None;
                        symbol_state.last_trade_quantity = None;
                        symbol_state.bids.clear();
                        symbol_state.asks.clear();
                        symbol_state.prev_mid = None;
                        symbol_state.recent_trades.clear();
                        symbol_state.last_updated = None;
                        symbol_state.last_updated_at = None;
                        symbol_state.last_bbo_update_at = None;
                    }
                }
            }
            MarketEvent::MarkPrice {
                symbol,
                price,
                timestamp,
            } => {
                let symbol_state = self.symbols.entry(symbol).or_default();
                symbol_state.prev_mid = symbol_state.mid_price();
                symbol_state.mark_price = Some(price);
                symbol_state.last_updated = Some(timestamp);
                symbol_state.last_updated_at = Some(Instant::now());
                self.last_updated = Some(timestamp);
                self.last_updated_at = Some(Instant::now());
            }
            MarketEvent::SpotPrice {
                symbol,
                price,
                timestamp,
            } => {
                let symbol_state = self.symbols.entry(symbol).or_default();
                symbol_state.spot_price = Some(price);
                symbol_state.last_updated = Some(timestamp);
                symbol_state.last_updated_at = Some(Instant::now());
                self.last_updated = Some(timestamp);
                self.last_updated_at = Some(Instant::now());
            }
            MarketEvent::BestBidAsk {
                symbol,
                bid,
                ask,
                bid_size,
                ask_size,
                timestamp,
            } => {
                let symbol_state = self.symbols.entry(symbol).or_default();
                symbol_state.prev_mid = symbol_state.mid_price();
                symbol_state.best_bid = Some(bid);
                symbol_state.best_ask = Some(ask);
                if bid_size.is_some() {
                    symbol_state.bbo_bid_size = bid_size;
                }
                if ask_size.is_some() {
                    symbol_state.bbo_ask_size = ask_size;
                }
                symbol_state.last_updated = Some(timestamp);
                symbol_state.last_updated_at = Some(Instant::now());
                symbol_state.last_bbo_update_at = Some(Instant::now());
                self.last_updated = Some(timestamp);
                self.last_updated_at = Some(Instant::now());
            }
            MarketEvent::FundingRate {
                symbol,
                rate,
                timestamp: _,
            } => {
                let symbol_state = self.symbols.entry(symbol).or_default();
                symbol_state.funding_rate = Some(rate);
            }
            MarketEvent::Trade {
                symbol,
                price,
                quantity,
                timestamp,
                taker_side,
            } => {
                let symbol_state = self.symbols.entry(symbol).or_default();
                symbol_state.last_trade_price = Some(price);
                symbol_state.last_trade_quantity = Some(quantity);
                symbol_state.recent_trades.push_back(RecentTrade {
                    quantity,
                    timestamp,
                    taker_side,
                });
                while symbol_state.recent_trades.len() > MAX_RECENT_TRADES {
                    symbol_state.recent_trades.pop_front();
                }
                maybe_shrink_recent_trades(&mut symbol_state.recent_trades);
                symbol_state.last_updated = Some(timestamp);
                symbol_state.last_updated_at = Some(Instant::now());
                self.last_updated = Some(timestamp);
                self.last_updated_at = Some(Instant::now());
            }
            MarketEvent::OrderBookSnapshot {
                symbol,
                bids,
                asks,
                timestamp,
            } => {
                let symbol_state = self.symbols.entry(symbol).or_default();
                symbol_state.bids = bids;
                symbol_state.asks = asks;
                prune_book_depth(&mut symbol_state.bids, self.max_orderbook_depth, true);
                prune_book_depth(&mut symbol_state.asks, self.max_orderbook_depth, false);
                if let Some((best_bid, bid_qty)) = symbol_state.bids.iter().next_back() {
                    symbol_state.best_bid = Some(*best_bid);
                    symbol_state.bbo_bid_size = Some(*bid_qty);
                }
                if let Some((best_ask, ask_qty)) = symbol_state.asks.iter().next() {
                    symbol_state.best_ask = Some(*best_ask);
                    symbol_state.bbo_ask_size = Some(*ask_qty);
                }
                symbol_state.last_updated = Some(timestamp);
                symbol_state.last_updated_at = Some(Instant::now());
                symbol_state.last_bbo_update_at = Some(Instant::now());
                self.last_updated = Some(timestamp);
                self.last_updated_at = Some(Instant::now());
            }
            MarketEvent::OrderBookUpdate {
                symbol,
                bids,
                asks,
                timestamp,
            } => {
                let symbol_state = self.symbols.entry(symbol).or_default();
                for (price, quantity) in bids {
                    if quantity.is_zero() {
                        symbol_state.bids.remove(&price);
                    } else {
                        symbol_state.bids.insert(price, quantity);
                    }
                }
                for (price, quantity) in asks {
                    if quantity.is_zero() {
                        symbol_state.asks.remove(&price);
                    } else {
                        symbol_state.asks.insert(price, quantity);
                    }
                }
                prune_book_depth(&mut symbol_state.bids, self.max_orderbook_depth, true);
                prune_book_depth(&mut symbol_state.asks, self.max_orderbook_depth, false);
                if let Some((best_bid, bid_qty)) = symbol_state.bids.iter().next_back() {
                    symbol_state.best_bid = Some(*best_bid);
                    symbol_state.bbo_bid_size = Some(*bid_qty);
                }
                if let Some((best_ask, ask_qty)) = symbol_state.asks.iter().next() {
                    symbol_state.best_ask = Some(*best_ask);
                    symbol_state.bbo_ask_size = Some(*ask_qty);
                }
                symbol_state.last_updated = Some(timestamp);
                symbol_state.last_updated_at = Some(Instant::now());
                symbol_state.last_bbo_update_at = Some(Instant::now());
                self.last_updated = Some(timestamp);
                self.last_updated_at = Some(Instant::now());
            }
        }
    }
}

#[derive(Default)]
pub struct SymbolMarketState {
    pub mark_price: Option<Decimal>,
    pub spot_price: Option<Decimal>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    /// BBO queue depth at the best bid, when published by the venue.
    pub bbo_bid_size: Option<Decimal>,
    /// BBO queue depth at the best ask, when published by the venue.
    pub bbo_ask_size: Option<Decimal>,
    /// Latest periodic funding rate (positive = longs pay shorts).
    pub funding_rate: Option<Decimal>,
    pub last_trade_price: Option<Decimal>,
    pub last_trade_quantity: Option<Decimal>,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
    pub prev_mid: Option<Decimal>,
    pub recent_trades: VecDeque<RecentTrade>,
    pub last_updated: Option<DateTime<Utc>>,
    pub last_updated_at: Option<Instant>,
    pub last_bbo_update_at: Option<Instant>,
}

impl SymbolMarketState {
    pub fn mid_price(&self) -> Option<Decimal> {
        match (self.best_bid, self.best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::from(2u64)),
            _ => None,
        }
    }

    /// BBO size-weighted mid-price (microprice).
    /// Formula: bid * (ask_size / total) + ask * (bid_size / total)
    /// Returns None when BBO prices or sizes are absent.
    pub fn microprice(&self) -> Option<Decimal> {
        let bid = self.best_bid?;
        let ask = self.best_ask?;
        let bid_size = self.bbo_bid_size?;
        let ask_size = self.bbo_ask_size?;
        let total = bid_size + ask_size;
        if total.is_zero() {
            return None;
        }
        // Inverted weights: large ask queue → price should be near bid (ask being absorbed)
        Some(bid * ask_size / total + ask * bid_size / total)
    }

    pub fn bbo_spread_bps(&self) -> Option<Decimal> {
        let bid = self.best_bid?;
        let ask = self.best_ask?;
        if bid <= Decimal::ZERO || ask <= Decimal::ZERO {
            return None;
        }
        let mid = (bid + ask) / Decimal::from(2u64);
        if mid <= Decimal::ZERO {
            return None;
        }
        Some((ask - bid) / mid * Decimal::from(10_000u64))
    }
}

pub fn prune_book_depth(book: &mut BTreeMap<Decimal, Decimal>, max_depth: usize, descending: bool) {
    if max_depth == 0 || book.len() <= max_depth {
        return;
    }

    let removed_levels = book.len().saturating_sub(max_depth);
    let retained = if descending {
        book.iter()
            .rev()
            .take(max_depth)
            .map(|(price, quantity)| (*price, *quantity))
            .collect::<Vec<_>>()
    } else {
        book.iter()
            .take(max_depth)
            .map(|(price, quantity)| (*price, *quantity))
            .collect::<Vec<_>>()
    };

    book.clear();
    for (price, quantity) in retained {
        book.insert(price, quantity);
    }
}

fn maybe_shrink_recent_trades(trades: &mut VecDeque<RecentTrade>) {
    let len = trades.len();
    let capacity = trades.capacity();
    if capacity > len.saturating_mul(2).max(2_048) {
        trades.shrink_to_fit();
    }
}
