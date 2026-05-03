use std::{collections::HashMap, fs, path::Path, str::FromStr, sync::Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use rust_decimal::Decimal;

use crate::{
    config::StorageConfig,
    core::state::BotState,
    domain::{
        BookSnapshot, CancelEvent, CrossVenueBasisSample, Fill, FillStorageTelemetry,
        FundingPaymentEvent, HedgeSimulationEvent, HedgeSimulationSnapshot, LatencySample,
        MidPriceSample, OrderRejectionEvent, Position, QuoteFilterEvent, QuotePlacementEvent,
        StrategySnapshot,
    },
};

pub struct FillStore {
    connection: Mutex<Connection>,
}

impl FillStore {
    pub fn from_config(config: Option<&StorageConfig>) -> Result<Option<Self>> {
        let Some(config) = config else {
            return Ok(None);
        };
        if !config.enabled {
            return Ok(None);
        }

        let db_path = Path::new(&config.db_path);
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create storage directory {}", parent.display())
                })?;
            }
        }

        let connection = Connection::open(db_path)
            .with_context(|| format!("failed to open fill store at {}", db_path.display()))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price TEXT NOT NULL,
                quantity TEXT NOT NULL,
                is_simulated INTEGER NOT NULL,
                position_quantity_after TEXT NOT NULL,
                entry_price_after TEXT NOT NULL,
                realized_pnl_after TEXT NOT NULL,
                unrealized_pnl_after TEXT NOT NULL,
                total_pnl_after TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS global_pnl_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                is_simulated INTEGER NOT NULL,
                realized_pnl TEXT NOT NULL,
                unrealized_pnl TEXT NOT NULL,
                total_pnl TEXT NOT NULL,
                account_equity TEXT NOT NULL,
                fill_symbol TEXT,
                fill_side TEXT
            );
            CREATE TABLE IF NOT EXISTS strategy_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                is_simulated INTEGER NOT NULL,
                decision TEXT NOT NULL,
                skip_reason TEXT,
                current_position TEXT NOT NULL,
                position_price TEXT NOT NULL,
                position_notional TEXT NOT NULL,
                has_position INTEGER NOT NULL,
                unwind_only INTEGER NOT NULL,
                stale_maker_unwind INTEGER NOT NULL,
                emergency_unwind INTEGER NOT NULL,
                degraded INTEGER NOT NULL,
                best_bid TEXT,
                best_ask TEXT,
                mid_price TEXT,
                mark_price TEXT,
                spot_price TEXT,
                bbo_spread_bps TEXT,
                bbo_bid_size TEXT,
                bbo_ask_size TEXT,
                price_index TEXT,
                raw_volatility TEXT,
                volatility TEXT,
                inventory_lean_bps TEXT,
                volume_imbalance TEXT,
                flow_direction TEXT,
                inventory_skew TEXT,
                recent_trade_count INTEGER,
                regime TEXT,
                regime_intensity TEXT,
                ob_imbalance TEXT,
                consecutive_flow_spike INTEGER,
                microprice TEXT,
                fill_rate_skew TEXT,
                vpin TEXT,
                funding_lean TEXT,
                kappa_estimate TEXT,
                post_fill_widen_bid TEXT,
                post_fill_widen_ask TEXT,
                flow_spike_widen_multiplier TEXT,
                toxic_regime_persistence_secs INTEGER,
                bid_lvl0_multiplier TEXT,
                ask_lvl0_multiplier TEXT,
                generated_quote_count INTEGER NOT NULL,
                desired_order_count INTEGER NOT NULL,
                bid_count INTEGER NOT NULL,
                ask_count INTEGER NOT NULL,
                total_bid_qty TEXT NOT NULL,
                total_ask_qty TEXT NOT NULL,
                top_bid TEXT,
                top_ask TEXT,
                account_equity TEXT NOT NULL,
                total_pnl TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS quote_filter_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                is_simulated INTEGER NOT NULL,
                reason TEXT NOT NULL,
                side TEXT NOT NULL,
                level_index INTEGER NOT NULL,
                is_unwind INTEGER NOT NULL,
                unwind_only INTEGER NOT NULL,
                stale_maker_unwind INTEGER NOT NULL,
                emergency_unwind INTEGER NOT NULL,
                current_position TEXT NOT NULL,
                raw_quantity TEXT NOT NULL,
                effective_quantity TEXT,
                remaining_capacity TEXT,
                price TEXT,
                notional TEXT,
                quote_min_trade_amount TEXT,
                best_bid TEXT,
                best_ask TEXT
            );
            CREATE TABLE IF NOT EXISTS quote_placements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                level_index INTEGER NOT NULL,
                price TEXT,
                quantity TEXT NOT NULL,
                order_id TEXT,
                client_order_key TEXT NOT NULL UNIQUE,
                placed_or_replaced TEXT NOT NULL,
                decision_ts TEXT NOT NULL,
                sent_ts TEXT NOT NULL,
                ack_ts TEXT,
                mid_price TEXT,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS book_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                mid_price TEXT,
                best_bid TEXT,
                best_ask TEXT,
                bids_json TEXT NOT NULL,
                asks_json TEXT NOT NULL,
                trigger TEXT NOT NULL,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cancel_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                level_index INTEGER,
                order_id TEXT,
                reason TEXT NOT NULL,
                time_alive_ms INTEGER,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mid_price_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                mid_price TEXT NOT NULL,
                best_bid TEXT,
                best_ask TEXT,
                mark_price TEXT,
                spot_price TEXT,
                source TEXT NOT NULL,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS order_rejections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                level_index INTEGER,
                price TEXT,
                quantity TEXT NOT NULL,
                reason TEXT NOT NULL,
                stage TEXT NOT NULL,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS latency_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                side TEXT,
                level_index INTEGER,
                decision_ts TEXT NOT NULL,
                sent_ts TEXT NOT NULL,
                ack_ts TEXT,
                client_order_key TEXT,
                order_id TEXT,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS funding_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT,
                payment_type TEXT NOT NULL,
                amount TEXT NOT NULL,
                note TEXT,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cross_venue_basis_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                primary_venue TEXT NOT NULL,
                hedge_venue TEXT NOT NULL,
                primary_bid TEXT NOT NULL,
                primary_ask TEXT NOT NULL,
                primary_mid TEXT NOT NULL,
                hedge_bid TEXT NOT NULL,
                hedge_ask TEXT NOT NULL,
                hedge_mid TEXT NOT NULL,
                basis_mid_bps TEXT NOT NULL,
                basis_open_buy_primary_bps TEXT NOT NULL,
                basis_open_sell_primary_bps TEXT NOT NULL,
                primary_mark_price TEXT,
                primary_spot_price TEXT,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hedge_simulation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                hedge_venue TEXT NOT NULL,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                primary_fill_side TEXT NOT NULL,
                primary_fill_price TEXT NOT NULL,
                primary_fill_qty TEXT NOT NULL,
                hedge_side TEXT NOT NULL,
                hedge_price TEXT,
                hedge_qty TEXT NOT NULL,
                basis_mid_bps TEXT,
                note TEXT,
                is_simulated INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hedge_simulation_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                hedge_venue TEXT NOT NULL,
                trigger TEXT NOT NULL,
                primary_position TEXT NOT NULL,
                primary_entry_price TEXT NOT NULL,
                primary_mark_price TEXT,
                hedge_position TEXT NOT NULL,
                hedge_entry_price TEXT NOT NULL,
                hedge_mid_price TEXT,
                net_position TEXT NOT NULL,
                primary_unrealized_pnl TEXT NOT NULL,
                hedge_unrealized_pnl TEXT NOT NULL,
                hedge_realized_pnl TEXT NOT NULL,
                total_pnl TEXT NOT NULL,
                is_simulated INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_payments_unique
            ON funding_payments (ts, symbol, payment_type, amount, note, is_simulated);",
        )?;
        ensure_column(&connection, "fills", "quote_placed_ts", "TEXT")?;
        ensure_column(&connection, "fills", "time_to_fill_ms", "INTEGER")?;
        ensure_column(&connection, "fills", "pre_fill_mid_drift_bps", "TEXT")?;
        ensure_column(&connection, "fills", "fee_paid", "TEXT")?;
        ensure_column(&connection, "fills", "funding_paid", "TEXT")?;

        Ok(Some(Self {
            connection: Mutex::new(connection),
        }))
    }

    pub fn insert_fill(
        &self,
        fill: &Fill,
        state: &BotState,
        is_simulated: bool,
        telemetry: Option<&FillStorageTelemetry>,
    ) -> Result<()> {
        let position = state.positions.get(&fill.symbol);
        let position_quantity_after = position
            .map(|position| position.quantity.to_string())
            .unwrap_or_else(|| "0".to_string());
        let entry_price_after = position
            .map(|position| position.entry_price.to_string())
            .unwrap_or_else(|| "0".to_string());
        let realized_pnl_after = position
            .map(|position| position.realized_pnl.to_string())
            .unwrap_or_else(|| "0".to_string());
        let unrealized_pnl_after = position
            .map(|position| position.unrealized_pnl.to_string())
            .unwrap_or_else(|| "0".to_string());
        let fee_paid = telemetry
            .and_then(|details| details.fee_paid)
            .map(|v| v.to_string());
        let funding_paid = telemetry
            .and_then(|details| details.funding_paid)
            .map(|v| v.to_string());
        let pre_fill_mid_drift_bps = telemetry
            .and_then(|details| details.pre_fill_mid_drift_bps)
            .map(|v| v.to_string());
        let quote_placed_ts = telemetry
            .and_then(|details| details.quote_placed_ts)
            .map(|ts| ts.to_rfc3339());
        let time_to_fill_ms = telemetry.and_then(|details| details.time_to_fill_ms);

        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO fills (
                ts, symbol, side, price, quantity, is_simulated,
                position_quantity_after, entry_price_after,
                realized_pnl_after, unrealized_pnl_after, total_pnl_after,
                quote_placed_ts, time_to_fill_ms, pre_fill_mid_drift_bps, fee_paid, funding_paid
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                fill.timestamp.to_rfc3339(),
                fill.symbol,
                format!("{:?}", fill.side),
                fill.price.to_string(),
                fill.quantity.to_string(),
                if is_simulated { 1 } else { 0 },
                position_quantity_after,
                entry_price_after,
                realized_pnl_after,
                unrealized_pnl_after,
                state.effective_total_pnl().to_string(),
                quote_placed_ts,
                time_to_fill_ms,
                pre_fill_mid_drift_bps,
                fee_paid,
                funding_paid,
            ],
        )?;
        self.insert_global_pnl_snapshot_locked(
            &connection,
            fill.timestamp.to_rfc3339(),
            state,
            is_simulated,
            Some(fill.symbol.as_str()),
            Some(format!("{:?}", fill.side)),
        )?;
        Ok(())
    }

    pub fn insert_global_pnl_snapshot(
        &self,
        ts: String,
        state: &BotState,
        is_simulated: bool,
        fill_symbol: Option<&str>,
        fill_side: Option<String>,
    ) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        self.insert_global_pnl_snapshot_locked(
            &connection,
            ts,
            state,
            is_simulated,
            fill_symbol,
            fill_side,
        )
    }

    fn insert_global_pnl_snapshot_locked(
        &self,
        connection: &Connection,
        ts: String,
        state: &BotState,
        is_simulated: bool,
        fill_symbol: Option<&str>,
        fill_side: Option<String>,
    ) -> Result<()> {
        let (realized_pnl, unrealized_pnl) = aggregate_pnl(state);
        connection.execute(
            "INSERT INTO global_pnl_snapshots (
                ts, is_simulated, realized_pnl, unrealized_pnl, total_pnl,
                account_equity, fill_symbol, fill_side
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                ts,
                if is_simulated { 1 } else { 0 },
                realized_pnl.to_string(),
                unrealized_pnl.to_string(),
                state.effective_total_pnl().to_string(),
                state.account_equity.to_string(),
                fill_symbol,
                fill_side,
            ],
        )?;
        Ok(())
    }

    pub fn load_latest_positions(&self, is_simulated: bool) -> Result<Vec<Position>> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        let mut statement = connection.prepare(
            "SELECT
                symbol,
                position_quantity_after,
                entry_price_after,
                realized_pnl_after
             FROM fills
             WHERE is_simulated = ?1
             ORDER BY id ASC",
        )?;
        let rows = statement.query_map(params![if is_simulated { 1 } else { 0 }], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        let mut latest_by_symbol: HashMap<String, Position> = HashMap::new();
        for row in rows {
            let (symbol, quantity, entry_price, realized_pnl) = row?;
            latest_by_symbol.insert(
                symbol.clone(),
                Position {
                    symbol,
                    quantity: Decimal::from_str(&quantity)?,
                    entry_price: Decimal::from_str(&entry_price)?,
                    realized_pnl: Decimal::from_str(&realized_pnl)?,
                    unrealized_pnl: Decimal::ZERO,
                    pnl_is_authoritative: false,
                    opened_at: None,
                },
            );
        }

        Ok(latest_by_symbol
            .into_values()
            .filter(|position| !position.quantity.is_zero() || !position.realized_pnl.is_zero())
            .collect())
    }

    pub fn insert_strategy_snapshot(&self, snapshot: &StrategySnapshot) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO strategy_snapshots (
                ts, symbol, is_simulated, decision, skip_reason,
                current_position, position_price, position_notional,
                has_position, unwind_only, stale_maker_unwind, emergency_unwind, degraded,
                best_bid, best_ask, mid_price, mark_price, spot_price,
                bbo_spread_bps, bbo_bid_size, bbo_ask_size,
                price_index, raw_volatility, volatility, inventory_lean_bps,
                volume_imbalance, flow_direction, inventory_skew, recent_trade_count,
                regime, regime_intensity, ob_imbalance, consecutive_flow_spike,
                microprice, fill_rate_skew, vpin, funding_lean, kappa_estimate,
                post_fill_widen_bid, post_fill_widen_ask, flow_spike_widen_multiplier,
                toxic_regime_persistence_secs, bid_lvl0_multiplier, ask_lvl0_multiplier,
                generated_quote_count, desired_order_count, bid_count, ask_count,
                total_bid_qty, total_ask_qty, top_bid, top_ask,
                account_equity, total_pnl
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5,
                ?6, ?7, ?8,
                ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18,
                ?19, ?20, ?21,
                ?22, ?23, ?24, ?25,
                ?26, ?27, ?28, ?29,
                ?30, ?31, ?32, ?33,
                ?34, ?35, ?36, ?37, ?38,
                ?39, ?40, ?41,
                ?42, ?43, ?44,
                ?45, ?46, ?47, ?48,
                ?49, ?50, ?51, ?52,
                ?53, ?54
            )",
            params![
                snapshot.ts.to_rfc3339(),
                snapshot.symbol,
                if snapshot.is_simulated { 1 } else { 0 },
                snapshot.decision,
                snapshot.skip_reason,
                snapshot.current_position.to_string(),
                snapshot.position_price.to_string(),
                snapshot.position_notional.to_string(),
                if snapshot.has_position { 1 } else { 0 },
                if snapshot.unwind_only { 1 } else { 0 },
                if snapshot.stale_maker_unwind { 1 } else { 0 },
                if snapshot.emergency_unwind { 1 } else { 0 },
                if snapshot.degraded { 1 } else { 0 },
                snapshot.best_bid.map(|v| v.to_string()),
                snapshot.best_ask.map(|v| v.to_string()),
                snapshot.mid_price.map(|v| v.to_string()),
                snapshot.mark_price.map(|v| v.to_string()),
                snapshot.spot_price.map(|v| v.to_string()),
                snapshot.bbo_spread_bps.map(|v| v.to_string()),
                snapshot.bbo_bid_size.map(|v| v.to_string()),
                snapshot.bbo_ask_size.map(|v| v.to_string()),
                snapshot.price_index.map(|v| v.to_string()),
                snapshot.raw_volatility.map(|v| v.to_string()),
                snapshot.volatility.map(|v| v.to_string()),
                snapshot.inventory_lean_bps.map(|v| v.to_string()),
                snapshot.volume_imbalance.map(|v| v.to_string()),
                snapshot.flow_direction.map(|v| v.to_string()),
                snapshot.inventory_skew.map(|v| v.to_string()),
                snapshot.recent_trade_count,
                snapshot.regime,
                snapshot.regime_intensity.map(|v| v.to_string()),
                snapshot.ob_imbalance.map(|v| v.to_string()),
                snapshot.consecutive_flow_spike,
                snapshot.microprice.map(|v| v.to_string()),
                snapshot.fill_rate_skew.map(|v| v.to_string()),
                snapshot.vpin.map(|v| v.to_string()),
                snapshot.funding_lean.map(|v| v.to_string()),
                snapshot.kappa_estimate.map(|v| v.to_string()),
                snapshot.post_fill_widen_bid.map(|v| v.to_string()),
                snapshot.post_fill_widen_ask.map(|v| v.to_string()),
                snapshot.flow_spike_widen_multiplier.map(|v| v.to_string()),
                snapshot.toxic_regime_persistence_secs,
                snapshot.bid_lvl0_multiplier.map(|v| v.to_string()),
                snapshot.ask_lvl0_multiplier.map(|v| v.to_string()),
                snapshot.generated_quote_count,
                snapshot.desired_order_count,
                snapshot.bid_count,
                snapshot.ask_count,
                snapshot.total_bid_qty.to_string(),
                snapshot.total_ask_qty.to_string(),
                snapshot.top_bid.map(|v| v.to_string()),
                snapshot.top_ask.map(|v| v.to_string()),
                snapshot.account_equity.to_string(),
                snapshot.total_pnl.to_string(),
            ],
        )?;
        Ok(())
    }

    pub fn insert_quote_filter_event(&self, event: &QuoteFilterEvent) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO quote_filter_events (
                ts, symbol, is_simulated, reason, side, level_index,
                is_unwind, unwind_only, stale_maker_unwind, emergency_unwind,
                current_position, raw_quantity, effective_quantity, remaining_capacity,
                price, notional, quote_min_trade_amount, best_bid, best_ask
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,
                ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18, ?19
            )",
            params![
                event.ts.to_rfc3339(),
                event.symbol,
                if event.is_simulated { 1 } else { 0 },
                event.reason,
                format!("{:?}", event.side),
                event.level_index,
                if event.is_unwind { 1 } else { 0 },
                if event.unwind_only { 1 } else { 0 },
                if event.stale_maker_unwind { 1 } else { 0 },
                if event.emergency_unwind { 1 } else { 0 },
                event.current_position.to_string(),
                event.raw_quantity.to_string(),
                event.effective_quantity.map(|v| v.to_string()),
                event.remaining_capacity.map(|v| v.to_string()),
                event.price.map(|v| v.to_string()),
                event.notional.map(|v| v.to_string()),
                event.quote_min_trade_amount.map(|v| v.to_string()),
                event.best_bid.map(|v| v.to_string()),
                event.best_ask.map(|v| v.to_string()),
            ],
        )?;
        Ok(())
    }

    pub fn insert_quote_placement(&self, event: &QuotePlacementEvent) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT OR REPLACE INTO quote_placements (
                ts, symbol, side, level_index, price, quantity, order_id, client_order_key,
                placed_or_replaced, decision_ts, sent_ts, ack_ts, mid_price, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                event.ts.to_rfc3339(),
                event.symbol,
                format!("{:?}", event.side),
                event.level_index,
                event.price.map(|v| v.to_string()),
                event.quantity.to_string(),
                event.order_id,
                event.client_order_key,
                event.placed_or_replaced,
                event.decision_ts.to_rfc3339(),
                event.sent_ts.to_rfc3339(),
                event.ack_ts.map(|ts| ts.to_rfc3339()),
                event.mid_price.map(|v| v.to_string()),
                if event.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn ack_quote_placement(
        &self,
        client_order_key: &str,
        order_id: Option<&str>,
        ack_ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "UPDATE quote_placements
             SET order_id = COALESCE(?1, order_id), ack_ts = ?2
             WHERE client_order_key = ?3",
            params![order_id, ack_ts.to_rfc3339(), client_order_key],
        )?;
        Ok(())
    }

    pub fn insert_book_snapshot(&self, snapshot: &BookSnapshot) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO book_snapshots (
                ts, symbol, mid_price, best_bid, best_ask, bids_json, asks_json, trigger, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                snapshot.ts.to_rfc3339(),
                snapshot.symbol,
                snapshot.mid_price.map(|v| v.to_string()),
                snapshot.best_bid.map(|v| v.to_string()),
                snapshot.best_ask.map(|v| v.to_string()),
                snapshot.bids_json,
                snapshot.asks_json,
                snapshot.trigger,
                if snapshot.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_cancel_event(&self, event: &CancelEvent) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO cancel_events (
                ts, symbol, side, level_index, order_id, reason, time_alive_ms, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                event.ts.to_rfc3339(),
                event.symbol,
                format!("{:?}", event.side),
                event.level_index,
                event.order_id,
                event.reason,
                event.time_alive_ms,
                if event.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_mid_price_sample(&self, sample: &MidPriceSample) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO mid_price_samples (
                ts, symbol, mid_price, best_bid, best_ask, mark_price, spot_price, source, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                sample.ts.to_rfc3339(),
                sample.symbol,
                sample.mid_price.to_string(),
                sample.best_bid.map(|v| v.to_string()),
                sample.best_ask.map(|v| v.to_string()),
                sample.mark_price.map(|v| v.to_string()),
                sample.spot_price.map(|v| v.to_string()),
                sample.source,
                if sample.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_order_rejection(&self, event: &OrderRejectionEvent) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO order_rejections (
                ts, symbol, side, level_index, price, quantity, reason, stage, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                event.ts.to_rfc3339(),
                event.symbol,
                format!("{:?}", event.side),
                event.level_index,
                event.price.map(|v| v.to_string()),
                event.quantity.to_string(),
                event.reason,
                event.stage,
                if event.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_latency_sample(&self, sample: &LatencySample) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO latency_samples (
                ts, symbol, action, side, level_index, decision_ts, sent_ts, ack_ts,
                client_order_key, order_id, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                sample.ts.to_rfc3339(),
                sample.symbol,
                sample.action,
                sample.side.map(|side| format!("{:?}", side)),
                sample.level_index,
                sample.decision_ts.to_rfc3339(),
                sample.sent_ts.to_rfc3339(),
                sample.ack_ts.map(|ts| ts.to_rfc3339()),
                sample.client_order_key,
                sample.order_id,
                if sample.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_funding_payment(&self, event: &FundingPaymentEvent) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT OR IGNORE INTO funding_payments (
                ts, symbol, payment_type, amount, note, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                event.ts.to_rfc3339(),
                event.symbol,
                event.payment_type,
                event.amount.to_string(),
                event.note,
                if event.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn latest_funding_payment_ts(&self, is_simulated: bool) -> Result<Option<DateTime<Utc>>> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        let mut statement =
            connection.prepare("SELECT MAX(ts) FROM funding_payments WHERE is_simulated = ?1")?;
        let latest: Option<String> =
            statement.query_row([if is_simulated { 1 } else { 0 }], |row| row.get(0))?;
        latest
            .map(|ts| {
                DateTime::parse_from_rfc3339(&ts)
                    .map(|dt| dt.with_timezone(&Utc))
                    .context("failed to parse stored funding payment timestamp")
            })
            .transpose()
    }

    pub fn insert_cross_venue_basis_sample(&self, sample: &CrossVenueBasisSample) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO cross_venue_basis_samples (
                ts, symbol, primary_venue, hedge_venue,
                primary_bid, primary_ask, primary_mid,
                hedge_bid, hedge_ask, hedge_mid,
                basis_mid_bps, basis_open_buy_primary_bps, basis_open_sell_primary_bps,
                primary_mark_price, primary_spot_price, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                sample.ts.to_rfc3339(),
                sample.symbol,
                sample.primary_venue,
                sample.hedge_venue,
                sample.primary_bid.to_string(),
                sample.primary_ask.to_string(),
                sample.primary_mid.to_string(),
                sample.hedge_bid.to_string(),
                sample.hedge_ask.to_string(),
                sample.hedge_mid.to_string(),
                sample.basis_mid_bps.to_string(),
                sample.basis_open_buy_primary_bps.to_string(),
                sample.basis_open_sell_primary_bps.to_string(),
                sample.primary_mark_price.map(|v| v.to_string()),
                sample.primary_spot_price.map(|v| v.to_string()),
                if sample.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_hedge_simulation_event(&self, event: &HedgeSimulationEvent) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO hedge_simulation_events (
                ts, symbol, hedge_venue, action, status,
                primary_fill_side, primary_fill_price, primary_fill_qty,
                hedge_side, hedge_price, hedge_qty, basis_mid_bps, note, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                event.ts.to_rfc3339(),
                event.symbol,
                event.hedge_venue,
                event.action,
                event.status,
                format!("{:?}", event.primary_fill_side),
                event.primary_fill_price.to_string(),
                event.primary_fill_qty.to_string(),
                format!("{:?}", event.hedge_side),
                event.hedge_price.map(|v| v.to_string()),
                event.hedge_qty.to_string(),
                event.basis_mid_bps.map(|v| v.to_string()),
                event.note,
                if event.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }

    pub fn insert_hedge_simulation_snapshot(
        &self,
        snapshot: &HedgeSimulationSnapshot,
    ) -> Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("fill store mutex poisoned"))?;
        connection.execute(
            "INSERT INTO hedge_simulation_snapshots (
                ts, symbol, hedge_venue, trigger,
                primary_position, primary_entry_price, primary_mark_price,
                hedge_position, hedge_entry_price, hedge_mid_price,
                net_position, primary_unrealized_pnl, hedge_unrealized_pnl,
                hedge_realized_pnl, total_pnl, is_simulated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                snapshot.ts.to_rfc3339(),
                snapshot.symbol,
                snapshot.hedge_venue,
                snapshot.trigger,
                snapshot.primary_position.to_string(),
                snapshot.primary_entry_price.to_string(),
                snapshot.primary_mark_price.map(|v| v.to_string()),
                snapshot.hedge_position.to_string(),
                snapshot.hedge_entry_price.to_string(),
                snapshot.hedge_mid_price.map(|v| v.to_string()),
                snapshot.net_position.to_string(),
                snapshot.primary_unrealized_pnl.to_string(),
                snapshot.hedge_unrealized_pnl.to_string(),
                snapshot.hedge_realized_pnl.to_string(),
                snapshot.total_pnl.to_string(),
                if snapshot.is_simulated { 1 } else { 0 },
            ],
        )?;
        Ok(())
    }
}

fn aggregate_pnl(state: &BotState) -> (Decimal, Decimal) {
    let realized: Decimal = state
        .positions
        .values()
        .map(|position| position.realized_pnl)
        .sum();
    let unrealized: Decimal = state
        .positions
        .values()
        .map(|position| position.unrealized_pnl)
        .sum();
    (realized, unrealized)
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut statement = connection.prepare(&pragma)?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let exists = columns
        .filter_map(|result| result.ok())
        .any(|name| name == column);
    if !exists {
        connection.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
    }
    Ok(())
}
