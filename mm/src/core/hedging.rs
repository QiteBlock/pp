use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::{prelude::Signed, Decimal};

use crate::{
    core::state::SymbolMarketState,
    domain::{ExternalVenue, Fill, HedgeSimulationEvent, HedgeSimulationSnapshot, Position, Side},
};

const HEDGE_QUOTE_STALE_AFTER: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default)]
struct SimulatedHedgePosition {
    quantity: Decimal,
    entry_price: Decimal,
    realized_pnl: Decimal,
}

pub struct HedgeSimulator {
    positions: HashMap<String, SimulatedHedgePosition>,
}

impl HedgeSimulator {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
        }
    }

    pub fn on_primary_fill(
        &mut self,
        fill: &Fill,
        market: Option<&SymbolMarketState>,
        primary_position: Option<&Position>,
        is_simulated: bool,
    ) -> (HedgeSimulationEvent, HedgeSimulationSnapshot) {
        let hedge_side = fill.side.opposite();
        let hedge_quote = market
            .and_then(|market| market.external_quotes.get(&ExternalVenue::Hyperliquid))
            .filter(|quote| {
                quote
                    .last_updated_at
                    .map(|updated| updated.elapsed() <= HEDGE_QUOTE_STALE_AFTER)
                    .unwrap_or(false)
            });
        let hedge_price = hedge_quote.map(|quote| match hedge_side {
            Side::Bid => quote.ask,
            Side::Ask => quote.bid,
        });
        let basis_mid_bps = market.and_then(|market| {
            let primary_mid = market.mid_price()?;
            let hedge_mid = hedge_quote?.mid_price();
            if primary_mid <= Decimal::ZERO || hedge_mid <= Decimal::ZERO {
                return None;
            }
            Some((primary_mid - hedge_mid) / hedge_mid * Decimal::from(10_000u64))
        });
        let status = if hedge_price.is_some() {
            "simulated"
        } else {
            "skipped_no_hl_quote"
        };
        if let Some(price) = hedge_price {
            self.apply_trade(&fill.symbol, hedge_side, price, fill.quantity);
        }
        let note = hedge_quote.map(|quote| {
            serde_json::json!({
                "hl_bid": quote.bid,
                "hl_ask": quote.ask,
                "hl_bid_size": quote.bid_size,
                "hl_ask_size": quote.ask_size,
            })
            .to_string()
        });

        let event = HedgeSimulationEvent {
            ts: fill.timestamp,
            symbol: fill.symbol.clone(),
            hedge_venue: "hyperliquid".to_string(),
            action: "hedge_primary_fill".to_string(),
            status: status.to_string(),
            primary_fill_side: fill.side,
            primary_fill_price: fill.price,
            primary_fill_qty: fill.quantity,
            hedge_side,
            hedge_price,
            hedge_qty: fill.quantity,
            basis_mid_bps,
            note,
            is_simulated,
        };
        let snapshot = self.snapshot(
            fill.timestamp,
            &fill.symbol,
            "primary_fill",
            market,
            primary_position,
            is_simulated,
        );
        (event, snapshot)
    }

    pub fn snapshot(
        &self,
        ts: DateTime<Utc>,
        symbol: &str,
        trigger: &str,
        market: Option<&SymbolMarketState>,
        primary_position: Option<&Position>,
        is_simulated: bool,
    ) -> HedgeSimulationSnapshot {
        let primary_position_qty = primary_position
            .map(|p| p.quantity)
            .unwrap_or(Decimal::ZERO);
        let primary_entry_price = primary_position
            .map(|p| p.entry_price)
            .unwrap_or(Decimal::ZERO);
        let primary_unrealized_pnl = primary_position
            .map(|p| p.unrealized_pnl)
            .unwrap_or(Decimal::ZERO);
        let primary_realized_pnl = primary_position
            .map(|p| p.realized_pnl)
            .unwrap_or(Decimal::ZERO);
        let primary_mark_price =
            market.and_then(|market| market.mark_price.or_else(|| market.mid_price()));
        let hedge_mid_price = market
            .and_then(|market| market.external_quotes.get(&ExternalVenue::Hyperliquid))
            .filter(|quote| {
                quote
                    .last_updated_at
                    .map(|updated| updated.elapsed() <= HEDGE_QUOTE_STALE_AFTER)
                    .unwrap_or(false)
            })
            .map(|quote| quote.mid_price());
        let hedge_position = self.positions.get(symbol).cloned().unwrap_or_default();
        let hedge_unrealized_pnl = hedge_mid_price
            .map(|mid| (mid - hedge_position.entry_price) * hedge_position.quantity)
            .unwrap_or(Decimal::ZERO);
        HedgeSimulationSnapshot {
            ts,
            symbol: symbol.to_string(),
            hedge_venue: "hyperliquid".to_string(),
            trigger: trigger.to_string(),
            primary_position: primary_position_qty,
            primary_entry_price,
            primary_mark_price,
            hedge_position: hedge_position.quantity,
            hedge_entry_price: hedge_position.entry_price,
            hedge_mid_price,
            net_position: primary_position_qty + hedge_position.quantity,
            primary_unrealized_pnl,
            hedge_unrealized_pnl,
            hedge_realized_pnl: hedge_position.realized_pnl,
            total_pnl: primary_realized_pnl
                + primary_unrealized_pnl
                + hedge_position.realized_pnl
                + hedge_unrealized_pnl,
            is_simulated,
        }
    }

    fn apply_trade(&mut self, symbol: &str, side: Side, price: Decimal, quantity: Decimal) {
        let position = self.positions.entry(symbol.to_string()).or_default();
        let fill_quantity = quantity * side.sign();
        let previous_quantity = position.quantity;
        let new_quantity = previous_quantity + fill_quantity;

        if previous_quantity.is_zero() || previous_quantity.signum() == fill_quantity.signum() {
            let previous_abs = previous_quantity.abs();
            let fill_abs = fill_quantity.abs();
            let total_abs = previous_abs + fill_abs;
            position.entry_price = if total_abs.is_zero() {
                Decimal::ZERO
            } else {
                ((position.entry_price * previous_abs) + (price * fill_abs)) / total_abs
            };
        } else {
            let closed_quantity = previous_quantity.abs().min(fill_quantity.abs());
            position.realized_pnl +=
                (price - position.entry_price) * closed_quantity * previous_quantity.signum();
            if new_quantity.is_zero() {
                position.entry_price = Decimal::ZERO;
            } else if new_quantity.signum() != previous_quantity.signum() {
                position.entry_price = price;
            }
        }

        position.quantity = new_quantity;
    }
}
