use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use rust_decimal::Decimal;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval, Duration, Instant},
};
use tracing::{info, warn};

use crate::{
    adapters::{
        exchange::ExchangeClient, feeds::binance::stream_binance_spot_prices,
        notifier::telegram::TelegramNotifier, storage::sqlite::FillStore,
    },
    config::{AppConfig, ParsedConfig},
    core::{
        analytics::{tracker::FillTracker, FillStats, MinuteStats},
        cleanup::{flatten_account_state, CleanupExchange},
        factors::FactorEngine,
        health::HealthTracker,
        quoting::avellaneda_stoikov::generate_quotes,
        risk::{proportional_vol_widening, run_security_checks, CircuitBreaker},
        runtime_control::RuntimeControl,
        state::BotState,
    },
    domain::{
        Fill, InstrumentMeta, MarketEvent, MarketRegime, OrderRequest, OrderType, PrivateEvent,
        Side, StrategySnapshot, TimeInForce,
    },
};

pub struct MarketMakingEngine<E> {
    config: AppConfig,
    parsed: ParsedConfig,
    exchange: Arc<E>,
    notifier: TelegramNotifier,
    fill_store: Arc<Option<FillStore>>,
    control: Arc<RuntimeControl>,
}

#[derive(Default)]
struct ReconResult {
    unexpected_orders: usize,
    missing_orders: usize,
}

struct ReconciliationPlan {
    to_cancel_order_ids: Vec<String>,
    to_place: Vec<OrderRequest>,
    changed_orders: usize,
    result: ReconResult,
}

impl ReconciliationPlan {
    fn should_update(&self) -> bool {
        !self.to_cancel_order_ids.is_empty() || !self.to_place.is_empty()
    }
}

#[derive(Default)]
struct GroupDiff {
    to_cancel_order_ids: Vec<String>,
    to_place: Vec<OrderRequest>,
    changed_count: usize,
    unexpected_orders: usize,
    missing_orders: usize,
}

impl<E> MarketMakingEngine<E>
where
    E: ExchangeClient + CleanupExchange + 'static,
{
    fn persist_strategy_snapshot(&self, snapshot: StrategySnapshot) -> Result<()> {
        if let Some(fill_store) = self.fill_store.as_ref().as_ref() {
            fill_store.insert_strategy_snapshot(&snapshot)?;
        }
        Ok(())
    }

    pub fn new(
        config: AppConfig,
        exchange: Arc<E>,
        notifier: TelegramNotifier,
        fill_store: Arc<Option<FillStore>>,
        control: Arc<RuntimeControl>,
    ) -> Self {
        let parsed = config
            .parsed()
            .expect("validated config should always produce a parsed config");
        Self {
            config,
            parsed,
            exchange,
            notifier,
            fill_store,
            control,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("engine run starting");
        let instruments = self.exchange.load_instruments().await?;
        info!(count = instruments.len(), "engine instruments loaded");
        let instrument_by_symbol: HashMap<String, InstrumentMeta> = instruments
            .into_iter()
            .map(|instrument| (instrument.symbol.clone(), instrument))
            .collect();

        let market_symbols: Vec<String> = self
            .config
            .pairs
            .iter()
            .filter(|pair| pair.enabled)
            .map(|pair| pair.symbol.clone())
            .collect();
        info!(symbols = ?market_symbols, "engine enabled market symbols");

        let initial_orders = self.exchange.fetch_open_orders().await?;
        info!(
            count = initial_orders.len(),
            "engine initial open orders fetched"
        );
        let (market_tx, mut market_rx) = mpsc::channel(1024);
        let (private_tx, mut private_rx) = mpsc::channel(512);
        info!("engine spawning all streams");
        let mut streams_task =
            self.spawn_all_streams(market_symbols.clone(), market_tx, private_tx);
        info!("engine streams task spawned");
        let mut state = BotState::new(
            self.parsed.venue.maker_fee_rate,
            self.parsed.runtime.max_orderbook_depth,
        );
        state.apply_private_event(PrivateEvent::OpenOrders(initial_orders));
        if self.config.runtime.dry_run && self.config.runtime.restore_dry_run_state {
            if let Some(fill_store) = self.fill_store.as_ref().as_ref() {
                let restored_positions = fill_store.load_latest_positions(true)?;
                let restored_count = restored_positions.len();
                for position in restored_positions {
                    state.apply_private_event(PrivateEvent::Position(position));
                }
                if restored_count > 0 {
                    info!(restored_count, "restored dry-run positions from fill store");
                }
            }
        } else if self.config.runtime.dry_run {
            info!("dry-run state restore disabled; starting from a clean baseline");
        }

        if self.config.runtime.dry_run {
            info!("dry-run mode active; order submission and cancel requests are disabled");
        }

        let mut factors = FactorEngine::default();
        let mut fill_tracker = FillTracker::default();
        let mut health = HealthTracker::default();
        if self.parsed.risk.max_correlated_position_usd <= Decimal::ZERO {
            info!("correlated position limit disabled; max_correlated_position_usd <= 0");
        }
        let mut circuit_breaker = CircuitBreaker::new(
            self.parsed.risk.circuit_breaker_cooldown_ms,
            self.parsed.risk.circuit_breaker_cooldown_ms * 8,
            self.parsed.risk.circuit_breaker_cooldown_ms * 4,
        );
        // Poll at 100ms so price-move triggered requotes fire promptly.
        // generation_min_interval_ms still controls actual reconcile cadence;
        // setting last_generation_at = None bypasses it for urgent requotes.
        let mut ticker = {
            let mut t = interval(Duration::from_millis(100));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t
        };
        let mut stats_ticker = interval(Duration::from_secs(60));
        let mut toxicity_ticker = {
            let mut t = interval(Duration::from_secs(5));
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t
        };
        let mut last_generation_at: Option<Instant> = None;
        let mut ticker_tick_count: u64 = 0;
        // Track the last-quoted price index per symbol to detect 3bps moves.
        let mut last_quoted_price: HashMap<String, Decimal> = HashMap::new();
        let mut last_breaker_message: Option<String> = None;
        let mut minute_stats = MinuteStats::default();
        let mut cumulative_traded_volume = Decimal::ZERO;
        // Issue 6: per-(symbol, side) cooldown after toxic fills.
        let mut side_cooldown: HashMap<(String, Side), Instant> = HashMap::new();
        // Per-symbol flow-spike unwind commitment: once set, forces unwind-only
        // quoting until the position is fully cleared, even if flow briefly dips
        // below the spike threshold (prevents oscillation around the threshold).
        let mut flow_spike_committed: HashSet<String> = HashSet::new();
        // Post-fill widen: tracks (symbol, side_to_widen) → Instant when widen expires.
        // After a fill on side S, widen the opposite side for post_fill_widen_secs.
        let mut post_fill_widen: HashMap<(String, Side), Instant> = HashMap::new();
        // Issue 9: pause quoting 3s after a WS reconnect to let state settle.
        let mut reconnect_pause_until: Option<Instant> = None;

        loop {
            tokio::select! {
                biased;
                maybe_private = private_rx.recv() => {
                    let event = maybe_private.context("private stream disconnected")?;
                    if matches!(event, PrivateEvent::StreamReconnected) {
                        health.record_ws_discrepancy();
                        // Issue 9: pause quoting 3 s after reconnect to let state settle.
                        reconnect_pause_until = Some(Instant::now() + Duration::from_millis(3000));
                        info!("grvt private stream reconnected; quoting paused for 3 s");
                    }
                    if let Some(fs) = self.apply_private_event_and_record(
                        &mut state,
                        event,
                        false,
                        &mut fill_tracker,
                        &mut minute_stats,
                        &mut cumulative_traded_volume,
                    ).await? {
                        if !fs.is_simulated {
                            last_generation_at = None;
                        }
                        // Issue 6: post-fill cooldown on toxic side.
                        if fs.toxicity_score > Decimal::from(2u64) {
                            info!(
                                symbol = %fs.fill_symbol,
                                side = ?fs.fill_side,
                                toxicity = %fs.toxicity_score,
                                "toxic fill; cooling down side for 3 s"
                            );
                            side_cooldown.insert((fs.fill_symbol.clone(), fs.fill_side), Instant::now());
                        }
                        // Post-fill widen: widen opposite side for N seconds.
                        let widen_secs = self.parsed.model.post_fill_widen_secs;
                        let widen_mult = self.parsed.model.post_fill_widen_multiplier;
                        if widen_secs > 0 && widen_mult > Decimal::ONE {
                            let opposite = fs.fill_side.opposite();
                            let expiry = Instant::now() + Duration::from_secs(widen_secs);
                            post_fill_widen.insert((fs.fill_symbol, opposite), expiry);
                        }
                    }
                }
                _ = ticker.tick() => {
                    ticker_tick_count += 1;
                    if ticker_tick_count % 50 == 0 {
                        info!(
                            tick_count = ticker_tick_count,
                            market_symbols = state.market.symbols.len(),
                            open_orders = state.open_orders.len(),
                            dry_run_orders = state.dry_run_orders.len(),
                            "ticker arm firing"
                        );
                    }
                    // Issue 9: hold off quoting during reconnect settle window.
                    if reconnect_pause_until.map(|t| Instant::now() < t).unwrap_or(false) {
                        continue;
                    }
                    if let Some(last_generation_at) = last_generation_at {
                        if last_generation_at.elapsed()
                            < Duration::from_millis(self.config.runtime.generation_min_interval_ms)
                        {
                            continue;
                        }
                    }

                    while let Ok(event) = private_rx.try_recv() {
                        if matches!(event, PrivateEvent::StreamReconnected) {
                            health.record_ws_discrepancy();
                            reconnect_pause_until = Some(Instant::now() + Duration::from_millis(3000));
                            info!("private stream reconnected during cycle drain; quoting paused for 3 s");
                        }
                        if let Some(fs) = self.apply_private_event_and_record(
                            &mut state,
                            event,
                            false,
                            &mut fill_tracker,
                            &mut minute_stats,
                            &mut cumulative_traded_volume,
                        ).await? {
                            if !fs.is_simulated {
                                last_generation_at = None;
                            }
                            if fs.toxicity_score > Decimal::from(2u64) {
                                info!(
                                    symbol = %fs.fill_symbol,
                                    side = ?fs.fill_side,
                                    toxicity = %fs.toxicity_score,
                                    "toxic fill during cycle drain; cooling down side for 3 s"
                                );
                                side_cooldown.insert((fs.fill_symbol.clone(), fs.fill_side), Instant::now());
                            }
                            // Post-fill widen on opposite side.
                            let widen_secs = self.parsed.model.post_fill_widen_secs;
                            let widen_mult = self.parsed.model.post_fill_widen_multiplier;
                            if widen_secs > 0 && widen_mult > Decimal::ONE {
                                let opposite = fs.fill_side.opposite();
                                let expiry = Instant::now() + Duration::from_secs(widen_secs);
                                post_fill_widen.insert((fs.fill_symbol, opposite), expiry);
                            }
                        }
                    }

                    if matches!(
                        last_breaker_message.as_deref(),
                        Some(message) if message.contains("open order count limit breached")
                    ) && state.open_orders.len() <= self.config.risk.max_open_orders
                    {
                        circuit_breaker.force_reset();
                        info!(
                            open_orders = state.open_orders.len(),
                            max_open_orders = self.config.risk.max_open_orders,
                            "reset open-order circuit breaker after count normalized"
                        );
                        last_breaker_message = None;
                    }

                    // Fix 4: REST fallback when WS market data has gone stale.
                    // Attempt one REST fetch per symbol before the security check trips
                    // the circuit breaker — avoids false-positive staleness outages.
                    {
                        let stale_after_ms = self.config.runtime.stale_market_data_ms;
                        let is_stale = state.market.last_updated_at.map_or(false, |instant| {
                            instant.elapsed().as_millis() > stale_after_ms.max(0) as u128
                        });
                        if is_stale {
                            warn!("market data stale; attempting REST snapshot fallback");
                            let snapshot = self.exchange.fetch_market_snapshot(&market_symbols).await;
                            if !snapshot.is_empty() {
                                info!(events = snapshot.len(), "REST snapshot refreshed stale market data");
                                for event in snapshot {
                                    state.apply_market_event(event);
                                }
                            }
                        }
                    }

                    if let Err(err) = run_security_checks(&self.config, &self.parsed, &state) {
                        circuit_breaker.trip(err.to_string());
                    } else {
                        circuit_breaker.reset();
                    }

                    if let Err(err) = circuit_breaker.ensure_closed() {
                        warn!(?err, "risk gate rejected quoting cycle");
                        let breaker_message = format!("{err:#}");
                        let suppress_cold_start_alert = state.market.last_updated.is_none()
                            && breaker_message.contains("no market data available yet");
                        let should_handle_trip = last_breaker_message.as_deref()
                            != Some(breaker_message.as_str());
                        if !self.config.runtime.dry_run && should_handle_trip {
                            // Always cancel all open orders when the breaker trips so that
                            // stale resting orders cannot keep filling while the bot is locked out.
                            self.exchange.cancel_all_orders().await?;
                            if should_flatten_on_breaker(&breaker_message) {
                                flatten_account_state(
                                    self.exchange.as_ref(),
                                    &self.notifier,
                                    "circuit-breaker",
                                    self.parsed.model.min_trade_amount,
                                ).await?;
                                let exchange_orders = self.exchange.fetch_open_orders().await?;
                                state.apply_private_event(PrivateEvent::OpenOrders(exchange_orders));
                                state.positions.clear();
                                state.refresh_after_external_position_reset();
                                for position in self.exchange.fetch_positions().await? {
                                    state.apply_private_event(PrivateEvent::Position(position));
                                }
                            }
                        }
                        if !suppress_cold_start_alert && should_handle_trip {
                            self.notifier
                                .send(format!("circuit breaker engaged: {breaker_message}"))
                                .await;
                        }
                        last_breaker_message = Some(breaker_message);
                        continue;
                    }
                    last_breaker_message = None;
                    info!(
                        market_symbols = state.market.symbols.len(),
                        open_orders = state.open_orders.len(),
                        dry_run_orders = state.dry_run_orders.len(),
                        "security checks passed; circuit breaker closed"
                    );
                    let paused = self.control.is_paused();
                    info!(paused, "pause check before desired-order build");
                    if paused {
                        continue;
                    }

                    let degraded = health.is_degraded();

                    // Build the set of symbols in emergency-unwind mode before generating
                    // orders so the generator can relax the min_trade_amount for those symbols.
                    let emergency_unwind_symbols: HashSet<String> = self
                        .config
                        .pairs
                        .iter()
                        .filter(|pair| pair.enabled)
                        .filter_map(|pair| {
                            let cycles = health.empty_unwind_cycles.get(&pair.symbol).copied().unwrap_or(0);
                            if cycles >= self.parsed.risk.emergency_unwind_cycles {
                                Some(pair.symbol.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    info!(
                        degraded,
                        emergency_unwind_symbols = emergency_unwind_symbols.len(),
                        side_cooldowns = side_cooldown.len(),
                        post_fill_widen_entries = post_fill_widen.len(),
                        "reached build_desired_orders call"
                    );
                    let desired = self.build_desired_orders(
                        &mut factors,
                        &state,
                        &instrument_by_symbol,
                        &fill_tracker,
                        degraded,
                        health.ws_discrepancy_count(),
                        health.rest_failure_count(),
                        &emergency_unwind_symbols,
                        &side_cooldown,
                        &mut flow_spike_committed,
                        &post_fill_widen,
                    )?;
                    // Online kappa: record a quote cycle for each enabled pair.
                    for pair in self.config.pairs.iter().filter(|p| p.enabled) {
                        fill_tracker.record_quote_cycle(&pair.symbol);
                    }
                    // Purge expired post-fill widen entries.
                    let now_inst = Instant::now();
                    post_fill_widen.retain(|_, &mut exp| now_inst < exp);

                    minute_stats.record_position_sample(&self.config, &state);
                    // Fix 1: alert when the bot stops quoting while holding a position.
                    {
                        let has_position = state
                            .positions
                            .values()
                            .any(|position| {
                                !position_is_effectively_flat(
                                    position.quantity,
                                    market_price_for_position(&state, position),
                                    self.parsed.model.min_trade_amount,
                                )
                            });
                        let alert_cycles = self.config.runtime.empty_quote_alert_cycles;
                        if health.update_empty_cycles(desired.len(), has_position, alert_cycles) {
                            let positions_desc: String = state
                                .positions
                                .values()
                                .filter(|position| {
                                    !position_is_effectively_flat(
                                        position.quantity,
                                        market_price_for_position(&state, position),
                                        self.parsed.model.min_trade_amount,
                                    )
                                })
                                .map(|p| format!("{} {}", p.symbol, p.quantity))
                                .collect::<Vec<_>>()
                                .join(", ");
                            warn!(
                                consecutive_cycles = alert_cycles,
                                positions = %positions_desc,
                                "bot generated zero orders while holding position"
                            );
                            self.notifier
                                .send(format!(
                                    "WARNING: bot generated 0 orders for {} consecutive cycles while holding positions: {}",
                                    alert_cycles, positions_desc,
                                ))
                                .await;
                        }
                    }

                    // Fix 3: update per-symbol emergency-unwind counters.
                    for pair in self.config.pairs.iter().filter(|p| p.enabled) {
                        let position = state
                            .positions
                            .get(&pair.symbol)
                            .map(|p| p.quantity)
                            .unwrap_or(Decimal::ZERO);
                        let parsed_pair = self.parsed.pairs.iter().find(|p| p.symbol == pair.symbol);
                        if let Some(parsed_pair) = parsed_pair {
                            let threshold = self.parsed.risk.emergency_unwind_threshold * parsed_pair.max_position_base;
                            let over_threshold = position.abs() > threshold;
                            let unwind_side = if position > Decimal::ZERO { Side::Ask } else { Side::Bid };
                            let unwind_orders = desired
                                .iter()
                                .filter(|o| o.symbol == pair.symbol && o.side == unwind_side)
                                .count();
                            let was_inactive = health.update_unwind_cycles(
                                &pair.symbol,
                                unwind_orders,
                                over_threshold,
                                self.parsed.risk.emergency_unwind_cycles,
                            );
                            if was_inactive {
                                warn!(
                                    symbol = %pair.symbol,
                                    position = %position,
                                    cycles = self.parsed.risk.emergency_unwind_cycles,
                                    "emergency unwind mode activated"
                                );
                                self.notifier
                                    .send(format!(
                                        "EMERGENCY UNWIND: {} position {} stuck for {} cycles; relaxing min trade amount",
                                        pair.symbol, position, self.parsed.risk.emergency_unwind_cycles,
                                    ))
                                    .await;
                            }
                        }
                    }

                    let current_orders = if self.config.runtime.dry_run {
                        &state.dry_run_orders
                    } else {
                        &state.open_orders
                    };
                    if !self.config.runtime.dry_run
                        && should_cancel_on_adverse_bbo_move(current_orders, &state)
                    {
                        info!("current BBO moved by more than 2 bps; cancelling stale resting orders");
                        self.exchange.cancel_all_orders().await?;
                        state.open_orders.clear();
                        last_generation_at = None;
                        continue;
                    }
                    let recon = build_reconciliation_plan(
                        &self.parsed,
                        &state,
                        current_orders,
                        &desired,
                    )?;
                    if recon.should_update() {
                        if self.config.runtime.dry_run {
                            state.apply_dry_run_plan(&recon.to_cancel_order_ids, &recon.to_place);
                            log_dry_run_inventory(&state);
                            log_dry_run_orders(&recon.to_place);
                            last_generation_at = Some(Instant::now());
                        } else {
                            // Set last_generation_at before the await so any ticker ticks
                            // that fire during cancel/place are blocked by the cooldown guard.
                            last_generation_at = Some(Instant::now());
                            // Cancel all existing orders atomically, then place new ones.
                            // A single cancel_all is cheaper and guarantees no stale orders
                            // survive (individual cancel+place races can leave orphans).
                            self.exchange.cancel_all_orders().await?;
                            // Clear local open-order state immediately so the next
                            // reconcile cycle doesn't see stale OPEN orders and
                            // generate wrong sizes while waiting for WS CANCELLED events.
                            state.open_orders.clear();
                            let filtered_to_place =
                                filter_orders_against_current_bbo(&self.parsed, &state, &recon.to_place);
                            if !filtered_to_place.is_empty() {
                                self.exchange.place_orders(filtered_to_place).await?;
                            } else {
                                info!("all candidate orders were filtered out by pre-placement BBO checks");
                            }
                            // Reset the cooldown timestamp again now that placement is done,
                            // so generation_min_interval_ms is measured from placement
                            // completion rather than start.
                            last_generation_at = Some(Instant::now());
                            // Update last quoted price so the 3bps move detector
                            // uses the price we just quoted, not an older one.
                            for pair in self.config.pairs.iter().filter(|p| p.enabled) {
                                if let Some(price) = state.market.symbols.get(&pair.symbol)
                                    .and_then(|s| s.spot_price.or(s.mark_price).or_else(|| s.mid_price()))
                                {
                                    last_quoted_price.insert(pair.symbol.clone(), price);
                                }
                            }
                        }
                    } else {
                        // No order changes needed; reset cooldown so we don't
                        // re-enter the generation block on every 100ms tick.
                        last_generation_at = Some(Instant::now());
                    }
                }
                _ = toxicity_ticker.tick() => {
                    // Periodically push the last known price into the fill_tracker so
                    // toxicity observations don't stall when the market WS goes quiet.
                    let now = Utc::now();
                    for pair in self.config.pairs.iter().filter(|p| p.enabled) {
                        if let Some(price) = state.market.symbols.get(&pair.symbol)
                            .and_then(|s| s.mark_price.or_else(|| s.mid_price()))
                        {
                            fill_tracker.observe_price(&pair.symbol, price, now);
                        }
                    }
                    fill_tracker.decay(now);
                }
                _ = stats_ticker.tick() => {
                    if let Some(fill_store) = self.fill_store.as_ref().as_ref() {
                        fill_store.insert_global_pnl_snapshot(
                            Utc::now().to_rfc3339(),
                            &state,
                            self.config.runtime.dry_run,
                            None,
                            None,
                        )?;
                    }
                    log_minute_stats(&self.config, &state, &minute_stats);
                    minute_stats = MinuteStats::default();
                }
                result = &mut streams_task => {
                    result.context("stream task join failed")??;
                    let err = anyhow::anyhow!("market/private streams stopped unexpectedly");
                    self.notifier.send(format!("{err:#}")).await;
                    return Err(err);
                }
                maybe_market = market_rx.recv() => {
                    let first_event = maybe_market.context("market stream disconnected")?;

                    // LIFO drain: collect all buffered events, deduplicate
                    // non-trade events by (symbol, kind) keeping only the
                    // freshest.  Trades are always processed in full for dry-run
                    // fill simulation accuracy.
                    let mut pending = vec![first_event];
                    while let Ok(e) = market_rx.try_recv() {
                        pending.push(e);
                    }
                    let mut deduped: HashMap<(String, u8), MarketEvent> = HashMap::new();
                    let mut trade_events: Vec<MarketEvent> = Vec::new();
                    for e in pending {
                        match &e {
                            MarketEvent::Trade { .. } => trade_events.push(e),
                            MarketEvent::BestBidAsk { symbol, .. } => { deduped.insert((symbol.clone(), 0), e); }
                            MarketEvent::MarkPrice { symbol, .. } => { deduped.insert((symbol.clone(), 1), e); }
                            MarketEvent::SpotPrice { symbol, .. } => { deduped.insert((symbol.clone(), 2), e); }
                            MarketEvent::OrderBookSnapshot { symbol, .. } => { deduped.insert((symbol.clone(), 3), e); }
                            MarketEvent::OrderBookUpdate { symbol, .. } => { deduped.insert((symbol.clone(), 4), e); }
                            MarketEvent::StreamReconnected { .. } => { deduped.insert((String::new(), 255), e); }
                            MarketEvent::FundingRate { symbol, .. } => { deduped.insert((symbol.clone(), 5), e); }
                        }
                    }
                    let all_events: Vec<MarketEvent> = deduped.into_values().chain(trade_events).collect();

                    for event in all_events {
                    if matches!(event, MarketEvent::Trade { .. }) {
                        minute_stats.trade_count += 1;
                    }
                    if matches!(event, MarketEvent::StreamReconnected { .. }) {
                        health.record_ws_discrepancy();
                    }
                    state.apply_market_event(event.clone());
                    if state.market.last_updated.is_some()
                        && matches!(
                            last_breaker_message.as_deref(),
                            Some(message)
                                if message.contains("no market data available yet")
                                    || message.contains("market data stale for")
                        )
                    {
                        circuit_breaker.force_reset();
                        last_breaker_message = None;
                        info!("reset market-data circuit breaker after fresh market data");
                    }
                    if let Some((symbol, reference_price, timestamp)) = market_reference(&state, &event) {
                        fill_tracker.observe_price(&symbol, reference_price, timestamp);
                    }
                    fill_tracker.decay(Utc::now());
                    if self.config.runtime.dry_run {
                        if let MarketEvent::Trade {
                              symbol,
                              price,
                              quantity,
                              taker_side,
                              timestamp,
                              ..
                          } = event
                            {
                                let simulated_fills =
                                    state.simulate_dry_run_trade(&symbol, price, quantity, timestamp);
                                let (missed_cross, near_miss) = log_missed_fill_opportunity(
                                    &state,
                                    &symbol,
                                    price,
                                  quantity,
                                  taker_side,
                                    timestamp,
                                    &simulated_fills,
                                );
                                if missed_cross {
                                    minute_stats.missed_crosses_count += 1;
                                }
                                if near_miss {
                                    minute_stats.near_misses_count += 1;
                                }
                                for fill in simulated_fills {
                                    let fs = self.apply_fill_and_record(
                                        &mut state,
                                        fill,
                                        true,
                                        &mut fill_tracker,
                                        &mut cumulative_traded_volume,
                                    ).await?;
                                    // Post-fill widen for simulated fills.
                                    let widen_secs = self.parsed.model.post_fill_widen_secs;
                                    let widen_mult = self.parsed.model.post_fill_widen_multiplier;
                                    if widen_secs > 0 && widen_mult > Decimal::ONE {
                                        let opposite = fs.fill_side.opposite();
                                        let expiry = Instant::now() + Duration::from_secs(widen_secs);
                                        post_fill_widen.insert((fs.fill_symbol.clone(), opposite), expiry);
                                    }
                                    minute_stats.accumulate_fill(fs);
                                }
                        }
                    }
                    } // end for event in all_events

                    // Force requote if any enabled symbol's price has moved >= 3bps
                    // since the last reconcile.  Clear last_generation_at so the
                    // next 100ms tick bypasses generation_min_interval_ms.
                    'price_check: for pair in self.config.pairs.iter().filter(|p| p.enabled) {
                        if let Some(new_price) = state.market.symbols.get(&pair.symbol)
                            .and_then(|s| s.spot_price.or(s.mark_price).or_else(|| s.mid_price()))
                        {
                            if let Some(&old_price) = last_quoted_price.get(&pair.symbol) {
                                if old_price > Decimal::ZERO {
                                    let move_bps = ((new_price - old_price) / old_price).abs()
                                        * Decimal::from(10_000u64);
                                    if move_bps >= Decimal::new(3, 0) {
                                        // Issue 4: only bypass the interval guard if at least
                                        // 500 ms have elapsed since last generation, preventing
                                        // 3bps ticks in a volatile market from cycling at 10 Hz.
                                        let can_force = last_generation_at
                                            .map(|t| t.elapsed() >= Duration::from_millis(500))
                                            .unwrap_or(true);
                                        if can_force {
                                            last_generation_at = None;
                                        }
                                        break 'price_check;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn apply_private_event_and_record(
        &self,
        state: &mut BotState,
        event: PrivateEvent,
        force_simulated: bool,
        fill_tracker: &mut FillTracker,
        minute_stats: &mut MinuteStats,
        cumulative_traded_volume: &mut Decimal,
    ) -> Result<Option<FillStats>> {
        match event {
            PrivateEvent::Fill(fill) => {
                let fs = self
                    .apply_fill_and_record(
                        state,
                        fill,
                        force_simulated,
                        fill_tracker,
                        cumulative_traded_volume,
                    )
                    .await?;
                minute_stats.accumulate_fill(fs.clone());
                Ok(Some(fs))
            }
            other => {
                state.apply_private_event(other);
                Ok(None)
            }
        }
    }

    async fn apply_fill_and_record(
        &self,
        state: &mut BotState,
        fill: Fill,
        is_simulated: bool,
        fill_tracker: &mut FillTracker,
        cumulative_traded_volume: &mut Decimal,
    ) -> Result<FillStats> {
        let (level_index, spread_earned_bps) = infer_fill_analytics(state, &fill);
        fill_tracker.record_fill(
            fill.symbol.clone(),
            fill.side,
            level_index,
            fill.price,
            spread_earned_bps,
            fill.timestamp,
        );
        let toxicity_score = fill_tracker.toxicity_score(&fill.symbol, fill.side, level_index);
        let next_level_multiplier =
            fill_tracker.level_volume_multiplier(&fill.symbol, fill.side, level_index);

        state.apply_private_event(PrivateEvent::Fill(fill.clone()));
        if state
            .positions
            .get(&fill.symbol)
            .map(|position| position.quantity.is_zero())
            .unwrap_or(true)
        {
            if let Some(reference_price) =
                state
                    .market
                    .symbols
                    .get(&fill.symbol)
                    .and_then(|market| match fill.side {
                        Side::Bid => market
                            .best_ask
                            .or_else(|| market.mark_price)
                            .or_else(|| market.mid_price()),
                        Side::Ask => market
                            .best_bid
                            .or_else(|| market.mark_price)
                            .or_else(|| market.mid_price()),
                    })
            {
                fill_tracker.observe_price(&fill.symbol, reference_price, fill.timestamp);
            }
        }
        if let Some(fill_store) = self.fill_store.as_ref().as_ref() {
            fill_store.insert_fill(&fill, state, is_simulated)?;
        }

        let position = state.positions.get(&fill.symbol);
        let position_quantity = position
            .map(|position| position.quantity)
            .unwrap_or(Decimal::ZERO);
        let realized_pnl = position
            .map(|position| position.realized_pnl)
            .unwrap_or(Decimal::ZERO);
        let unrealized_pnl = position
            .map(|position| position.unrealized_pnl)
            .unwrap_or(Decimal::ZERO);
        let session_account_pnl = state.session_account_pnl();
        let effective_total_pnl = state.effective_total_pnl();
        let fill_notional = fill.quantity.abs() * fill.price;
        *cumulative_traded_volume += fill_notional;
        let pnl_per_volume_bps = if *cumulative_traded_volume > Decimal::ZERO {
            effective_total_pnl / *cumulative_traded_volume * Decimal::from(10_000u64)
        } else {
            Decimal::ZERO
        };

        info!(
            symbol = %fill.symbol,
            side = ?fill.side,
            level_index,
            price = %fill.price,
            quantity = %fill.quantity,
            spread_earned_bps = %spread_earned_bps,
            toxicity_score = %toxicity_score,
            next_level_multiplier = %next_level_multiplier,
            is_simulated,
            position_quantity = %position_quantity,
            realized_pnl = %realized_pnl,
            unrealized_pnl = %unrealized_pnl,
            account_equity = %state.account_equity,
            startup_account_equity = ?state.startup_account_equity,
            session_account_pnl = ?session_account_pnl,
            total_fees = %state.pnl.total_fees,
            total_pnl = %effective_total_pnl,
            "fill recorded"
        );

        let fill_kind = if is_simulated { "simulated" } else { "live" };
        self.notifier
            .send(format!(
                "{fill_kind} fill\nsymbol: {}\nside: {:?}\nlevel: {}\nprice: {}\nquantity: {}\nposition: {}\nrealized pnl: {}\nunrealized pnl: {}\naccount equity: {}\nstartup equity: {}\nsession pnl: {}\nfees: {}\ntotal pnl: {}\nsession volume: {}\npnl / volume (bps): {}",
                fill.symbol,
                fill.side,
                level_index,
                fill.price,
                fill.quantity,
                position_quantity,
                realized_pnl,
                unrealized_pnl,
                state.account_equity,
                state
                    .startup_account_equity
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
                session_account_pnl
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".to_string()),
                state.pnl.total_fees,
                effective_total_pnl,
                cumulative_traded_volume.round_dp(2),
                pnl_per_volume_bps.round_dp(2),
            ))
            .await;
        Ok(FillStats {
            spread_earned_bps,
            notional: fill_notional,
            is_toxic: toxicity_score > Decimal::ONE,
            is_simulated,
            toxicity_score,
            fill_symbol: fill.symbol.clone(),
            fill_side: fill.side,
        })
    }

    fn spawn_all_streams(
        &self,
        symbols: Vec<String>,
        market_sender: mpsc::Sender<MarketEvent>,
        private_sender: mpsc::Sender<PrivateEvent>,
    ) -> JoinHandle<Result<()>> {
        let exchange = self.exchange.clone();
        info!(symbols = ?symbols, "spawning exchange streams");
        // Build Binance symbol map from pair configs that have binance_symbol set.
        let binance_symbol_map: HashMap<String, String> = self
            .config
            .pairs
            .iter()
            .filter(|p| p.enabled)
            .filter_map(|p| {
                p.binance_symbol
                    .as_ref()
                    .map(|bs| (p.symbol.clone(), bs.clone()))
            })
            .collect();
        // Binance feed is independent: a GRVT disconnect must not kill it and
        // vice versa.  Spawn it as a fire-and-forget task that writes into the
        // same market channel.  Errors inside it are logged and reconnected
        // internally; we never propagate them to the engine loop.
        let binance_sender = market_sender.clone();
        tokio::spawn(async move {
            if let Err(e) = stream_binance_spot_prices(binance_symbol_map, binance_sender).await {
                tracing::warn!(err = %e, "binance feed task exited unexpectedly");
            }
        });

        tokio::spawn(async move {
            tokio::try_join!(
                exchange.stream_mark_prices(&symbols, market_sender.clone()),
                exchange.stream_spot_prices(&symbols, market_sender.clone()),
                exchange.stream_best_bid_ask(&symbols, market_sender.clone()),
                exchange.stream_trades(&symbols, market_sender.clone()),
                exchange.stream_orderbook(&symbols, market_sender),
                exchange.stream_private_data(private_sender),
            )?;
            Ok(())
        })
    }

    fn build_desired_orders(
        &self,
        factors: &mut FactorEngine,
        state: &BotState,
        instrument_by_symbol: &HashMap<String, InstrumentMeta>,
        fill_tracker: &FillTracker,
        degraded: bool,
        _ws_discrepancies: usize,
        _rest_failures: usize,
        emergency_unwind_symbols: &HashSet<String>,
        side_cooldown: &HashMap<(String, Side), Instant>,
        flow_spike_committed: &mut HashSet<String>,
        post_fill_widen: &HashMap<(String, Side), Instant>,
    ) -> Result<Vec<OrderRequest>> {
        let mut orders = Vec::new();
        let vol_cut = self.parsed.model.volatility_cut_threshold;
        let min_trade_amount = self.parsed.model.min_trade_amount;
        let n_trade_threshold = self.config.factors.n_trade_quote_threshold;
        let hard_toxic_stop_secs = 300u64;
        let min_level_multiplier = Decimal::new(5, 1);
        let non_unwind_size_reduction = Decimal::new(25, 2);
        let position_hold_timeout =
            ChronoDuration::seconds(self.parsed.risk.stale_position_timeout_secs as i64);
        let stale_close_slippage_cap_frac =
            self.parsed.risk.stale_position_close_slippage_cap_bps / Decimal::from(10_000u64);
        let stale_close_chunk_base = self.parsed.risk.stale_position_close_chunk_base;
        let enabled_pairs: Vec<_> = self
            .config
            .pairs
            .iter()
            .filter(|pair| pair.enabled)
            .collect();
        let now_utc = Utc::now();
        let mut stale_flatten_orders = Vec::new();
        let mut stale_maker_unwind_symbols = HashSet::new();

        if self.parsed.risk.stale_position_timeout_secs > 0 {
            for pair in &enabled_pairs {
                let Some(position) = state.positions.get(&pair.symbol) else {
                    continue;
                };
                let Some(opened_at) = position.opened_at else {
                    continue;
                };
                let position_age = now_utc - opened_at;
                if position_age <= position_hold_timeout {
                    continue;
                }

                let reference_price =
                    position_price_for_symbol(state, &pair.symbol, position.entry_price);
                if position.quantity == Decimal::ZERO {
                    continue;
                }

                let Some(instrument) = instrument_by_symbol.get(&pair.symbol) else {
                    warn!(
                        symbol = %pair.symbol,
                        position = %position.quantity,
                        position_age_secs = position_age.num_seconds(),
                        "stale position exceeded hold timeout but instrument metadata is missing; skipping forced flatten"
                    );
                    continue;
                };
                let total_abs_quantity = position.quantity.abs();
                let total_notional = total_abs_quantity * reference_price;
                if reference_price <= Decimal::ZERO || total_notional < min_trade_amount {
                    warn!(
                        symbol = %pair.symbol,
                        position = %position.quantity,
                        position_age_secs = position_age.num_seconds(),
                        reference_price = %reference_price,
                        total_notional = %total_notional,
                        min_trade_amount = %min_trade_amount,
                        "stale position exceeded hold timeout but is below IOC close threshold; falling back to unwind-only maker quoting"
                    );
                    stale_maker_unwind_symbols.insert(pair.symbol.clone());
                    continue;
                }

                let chunk_target = if stale_close_chunk_base > Decimal::ZERO {
                    total_abs_quantity.min(stale_close_chunk_base)
                } else {
                    total_abs_quantity
                };
                let min_notional_quantity =
                    (min_trade_amount / reference_price).max(Decimal::new(1, 9));
                let desired_close_quantity = chunk_target
                    .max(min_notional_quantity)
                    .min(total_abs_quantity);
                let quantity = quantize_order_quantity_for_checks(
                    desired_close_quantity,
                    instrument,
                    self.parsed.model.min_step_volume,
                );
                let chunk_notional = quantity * reference_price;
                if quantity <= Decimal::ZERO || chunk_notional < min_trade_amount {
                    warn!(
                        symbol = %pair.symbol,
                        position = %position.quantity,
                        position_age_secs = position_age.num_seconds(),
                        quantity = %quantity,
                        reference_price = %reference_price,
                        notional = %chunk_notional,
                        min_trade_amount = %min_trade_amount,
                        "stale position exceeded hold timeout but IOC close chunk is below minimum tradable threshold; falling back to unwind-only maker quoting"
                    );
                    stale_maker_unwind_symbols.insert(pair.symbol.clone());
                    continue;
                }

                let side = if position.quantity > Decimal::ZERO {
                    Side::Ask
                } else {
                    Side::Bid
                };
                let close_reference_price = state
                    .market
                    .symbols
                    .get(&pair.symbol)
                    .and_then(|market| {
                        market
                            .mid_price()
                            .or(market.mark_price)
                            .or(market.spot_price)
                            .or(market.best_bid)
                            .or(market.best_ask)
                    })
                    .unwrap_or(reference_price);
                let capped_limit_price = round_to_tick_for_side(
                    close_reference_price
                        * match side {
                            Side::Ask => Decimal::ONE - stale_close_slippage_cap_frac,
                            Side::Bid => Decimal::ONE + stale_close_slippage_cap_frac,
                        },
                    instrument.tick_size,
                    side,
                );
                warn!(
                    symbol = %pair.symbol,
                    position = %position.quantity,
                    position_age_secs = position_age.num_seconds(),
                    side = ?side,
                    quantity = %quantity,
                    reference_price = %reference_price,
                    limit_price = %capped_limit_price,
                    slippage_cap_bps = %self.parsed.risk.stale_position_close_slippage_cap_bps,
                    chunk_base = %stale_close_chunk_base,
                    "stale position exceeded hold timeout; forcing IOC limit flatten and skipping normal quote generation"
                );
                let order = OrderRequest {
                    symbol: pair.symbol.clone(),
                    contract_id: instrument.contract_id,
                    level_index: 0,
                    side,
                    order_type: OrderType::Limit,
                    time_in_force: TimeInForce::ImmediateOrCancel,
                    price: Some(capped_limit_price),
                    quantity,
                    post_only: false,
                };
                self.persist_strategy_snapshot(build_strategy_snapshot(
                    state,
                    &pair.symbol,
                    self.config.runtime.dry_run,
                    "stale_ioc_close",
                    None,
                    position.quantity,
                    reference_price,
                    true,
                    true,
                    false,
                    false,
                    degraded,
                    state.market.symbols.get(&pair.symbol),
                    None,
                    None,
                    None,
                    0,
                    std::slice::from_ref(&order),
                ))?;
                stale_flatten_orders.push(order);
            }
        }

        if !stale_flatten_orders.is_empty() {
            warn!(
                stale_order_count = stale_flatten_orders.len(),
                "returning stale-position IOC market flatten orders only for this cycle"
            );
            return Ok(stale_flatten_orders);
        }

        for pair in enabled_pairs {
            let Some(mut factor_snapshot) =
                factors.compute(&self.config, &self.parsed, pair, state, fill_tracker)
            else {
                info!(symbol = %pair.symbol, "factor snapshot unavailable; skipping symbol");
                let current_position = state
                    .positions
                    .get(&pair.symbol)
                    .map(|position| position.quantity)
                    .unwrap_or(Decimal::ZERO);
                let position_price = position_price_for_symbol(state, &pair.symbol, Decimal::ZERO);
                let has_position = !position_is_effectively_flat(
                    current_position,
                    position_price,
                    min_trade_amount,
                );
                self.persist_strategy_snapshot(build_strategy_snapshot(
                    state,
                    &pair.symbol,
                    self.config.runtime.dry_run,
                    "skip",
                    Some("factor_unavailable"),
                    current_position,
                    position_price,
                    has_position,
                    false,
                    stale_maker_unwind_symbols.contains(&pair.symbol),
                    false,
                    degraded,
                    state.market.symbols.get(&pair.symbol),
                    None,
                    None,
                    None,
                    0,
                    &[],
                ))?;
                continue;
            };
            // Populate post-fill widen multipliers.
            let now_inst = Instant::now();
            let widen_mult = self.parsed.model.post_fill_widen_multiplier;
            if widen_mult > Decimal::ONE {
                if post_fill_widen
                    .get(&(pair.symbol.clone(), Side::Bid))
                    .map(|&exp| now_inst < exp)
                    .unwrap_or(false)
                {
                    factor_snapshot.post_fill_widen_bid = widen_mult;
                }
                if post_fill_widen
                    .get(&(pair.symbol.clone(), Side::Ask))
                    .map(|&exp| now_inst < exp)
                    .unwrap_or(false)
                {
                    factor_snapshot.post_fill_widen_ask = widen_mult;
                }
            }
            let current_position = state
                .positions
                .get(&pair.symbol)
                .map(|position| position.quantity)
                .unwrap_or(Decimal::ZERO);
            let stale_maker_unwind = stale_maker_unwind_symbols.contains(&pair.symbol);
            let position_price =
                position_price_for_symbol(state, &pair.symbol, factor_snapshot.price_index);
            let has_position =
                !position_is_effectively_flat(current_position, position_price, min_trade_amount);
            let treat_as_position = has_position || stale_maker_unwind;
            let mut unwind_only = false;
            let bid_lvl0_multiplier =
                fill_tracker.level_volume_multiplier(&pair.symbol, Side::Bid, 0);
            let ask_lvl0_multiplier =
                fill_tracker.level_volume_multiplier(&pair.symbol, Side::Ask, 0);
            info!(
                symbol = %pair.symbol,
                position = %current_position,
                has_position,
                raw_volatility = %factor_snapshot.raw_volatility,
                spread_addon = %factor_snapshot.volatility,
                inventory_lean_bps = %factor_snapshot.inventory_lean_bps,
                regime = ?factor_snapshot.regime,
                regime_intensity = %factor_snapshot.regime_intensity,
                recent_trade_count = factor_snapshot.recent_trade_count,
                flow_direction = %factor_snapshot.flow_direction,
                bid_lvl0_multiplier = %bid_lvl0_multiplier,
                ask_lvl0_multiplier = %ask_lvl0_multiplier,
                "building desired orders for symbol"
            );
            if stale_maker_unwind {
                warn!(
                    symbol = %pair.symbol,
                    position = %current_position,
                    position_price = %position_price,
                    "stale residual position below IOC close threshold; forcing unwind-only maker flow"
                );
                unwind_only = true;
            }
            let toxic_regime_blocks_new_positions = factor_snapshot.regime
                == MarketRegime::TrendingToxic
                && factor_snapshot.regime_intensity
                    >= self
                        .parsed
                        .factors
                        .toxic_regime_block_new_positions_intensity
                && factor_snapshot.toxic_regime_persistence_secs
                    >= self.parsed.factors.toxic_regime_block_new_positions_secs;
            let sustained_fully_toxic = factor_snapshot.toxic_regime_persistence_secs
                >= hard_toxic_stop_secs
                && bid_lvl0_multiplier <= min_level_multiplier
                && ask_lvl0_multiplier <= min_level_multiplier;

            if factor_snapshot.recent_trade_count < n_trade_threshold {
                if treat_as_position {
                    warn!(
                        symbol = %pair.symbol,
                        position = %current_position,
                        recent_trade_count = factor_snapshot.recent_trade_count,
                        threshold = n_trade_threshold,
                        "recent trade count below threshold while holding position; switching to unwind-only quoting"
                    );
                    unwind_only = true;
                } else {
                    self.persist_strategy_snapshot(build_strategy_snapshot(
                        state,
                        &pair.symbol,
                        self.config.runtime.dry_run,
                        "skip",
                        Some("recent_trade_threshold"),
                        current_position,
                        position_price,
                        has_position,
                        false,
                        stale_maker_unwind,
                        false,
                        degraded,
                        state.market.symbols.get(&pair.symbol),
                        Some(&factor_snapshot),
                        Some(bid_lvl0_multiplier),
                        Some(ask_lvl0_multiplier),
                        0,
                        &[],
                    ))?;
                    continue;
                }
            }
            if toxic_regime_blocks_new_positions {
                if treat_as_position {
                    warn!(
                        symbol = %pair.symbol,
                        position = %current_position,
                        regime_intensity = %factor_snapshot.regime_intensity,
                        persistence_secs = factor_snapshot.toxic_regime_persistence_secs,
                        threshold_secs = self.parsed.factors.toxic_regime_block_new_positions_secs,
                        "persistent trending-toxic regime; switching to unwind-only quoting"
                    );
                    unwind_only = true;
                } else {
                    warn!(
                        symbol = %pair.symbol,
                        regime_intensity = %factor_snapshot.regime_intensity,
                        persistence_secs = factor_snapshot.toxic_regime_persistence_secs,
                        threshold_secs = self.parsed.factors.toxic_regime_block_new_positions_secs,
                        "persistent trending-toxic regime; blocking new positions"
                    );
                    self.persist_strategy_snapshot(build_strategy_snapshot(
                        state,
                        &pair.symbol,
                        self.config.runtime.dry_run,
                        "skip",
                        Some("persistent_trending_toxic"),
                        current_position,
                        position_price,
                        has_position,
                        false,
                        stale_maker_unwind,
                        false,
                        degraded,
                        state.market.symbols.get(&pair.symbol),
                        Some(&factor_snapshot),
                        Some(bid_lvl0_multiplier),
                        Some(ask_lvl0_multiplier),
                        0,
                        &[],
                    ))?;
                    continue;
                }
            }
            if sustained_fully_toxic {
                if treat_as_position {
                    warn!(
                        symbol = %pair.symbol,
                        position = %current_position,
                        toxic_persistence_secs = factor_snapshot.toxic_regime_persistence_secs,
                        hard_stop_secs = hard_toxic_stop_secs,
                        bid_lvl0_multiplier = %bid_lvl0_multiplier,
                        ask_lvl0_multiplier = %ask_lvl0_multiplier,
                        "all recent fills remain toxic with size pinned at the floor; switching to unwind-only quoting"
                    );
                    unwind_only = true;
                } else {
                    warn!(
                        symbol = %pair.symbol,
                        toxic_persistence_secs = factor_snapshot.toxic_regime_persistence_secs,
                        hard_stop_secs = hard_toxic_stop_secs,
                        bid_lvl0_multiplier = %bid_lvl0_multiplier,
                        ask_lvl0_multiplier = %ask_lvl0_multiplier,
                        "all recent fills remain toxic with size pinned at the floor; suppressing quotes entirely"
                    );
                    self.persist_strategy_snapshot(build_strategy_snapshot(
                        state,
                        &pair.symbol,
                        self.config.runtime.dry_run,
                        "skip",
                        Some("sustained_fully_toxic"),
                        current_position,
                        position_price,
                        has_position,
                        false,
                        stale_maker_unwind,
                        false,
                        degraded,
                        state.market.symbols.get(&pair.symbol),
                        Some(&factor_snapshot),
                        Some(bid_lvl0_multiplier),
                        Some(ask_lvl0_multiplier),
                        0,
                        &[],
                    ))?;
                    continue;
                }
            }
            if factor_snapshot.raw_volatility > vol_cut {
                if treat_as_position {
                    warn!(
                        symbol = %pair.symbol,
                        position = %current_position,
                        raw_volatility = %factor_snapshot.raw_volatility,
                        threshold = %vol_cut,
                        "volatility cut exceeded while holding position; switching to unwind-only quoting"
                    );
                    unwind_only = true;
                } else {
                    warn!(
                        symbol = %pair.symbol,
                        raw_volatility = %factor_snapshot.raw_volatility,
                        threshold = %vol_cut,
                        "volatility cut threshold exceeded; suppressing quotes"
                    );
                    self.persist_strategy_snapshot(build_strategy_snapshot(
                        state,
                        &pair.symbol,
                        self.config.runtime.dry_run,
                        "skip",
                        Some("volatility_cut"),
                        current_position,
                        position_price,
                        has_position,
                        false,
                        stale_maker_unwind,
                        false,
                        degraded,
                        state.market.symbols.get(&pair.symbol),
                        Some(&factor_snapshot),
                        Some(bid_lvl0_multiplier),
                        Some(ask_lvl0_multiplier),
                        0,
                        &[],
                    ))?;
                    continue;
                }
            }

            // Flow spike pause — latch-based to prevent oscillation.
            // Enter: ≥2 consecutive cycles above threshold.
            // Exit: position cleared to zero (commitment released only when flat).
            let flow_spike_threshold = self.parsed.factors.flow_spike_pause_threshold;
            if flow_spike_threshold > Decimal::ZERO {
                let spike_active = factor_snapshot.flow_direction.abs() > flow_spike_threshold
                    && factor_snapshot.consecutive_flow_spike >= 2;
                if spike_active {
                    factor_snapshot.flow_spike_widen_multiplier =
                        Decimal::ONE + factor_snapshot.flow_direction.abs().min(Decimal::ONE);
                }

                // Commit on new spike; release once the residual position is no longer tradeable.
                if spike_active && has_position {
                    if flow_spike_committed.insert(pair.symbol.clone()) {
                        warn!(
                            symbol = %pair.symbol,
                            position = %current_position,
                            flow_direction = %factor_snapshot.flow_direction,
                            threshold = %flow_spike_threshold,
                            consecutive_cycles = factor_snapshot.consecutive_flow_spike,
                            "sustained flow spike; committing to unwind-only until flat"
                        );
                    }
                } else if !has_position {
                    flow_spike_committed.remove(&pair.symbol);
                }

                if flow_spike_committed.contains(&pair.symbol) {
                    if has_position {
                        warn!(
                            symbol = %pair.symbol,
                            position = %current_position,
                            flow_direction = %factor_snapshot.flow_direction,
                            committed = true,
                            "flow spike committed; unwind-only quoting"
                        );
                        unwind_only = true;
                    }
                } else if spike_active {
                    warn!(
                        symbol = %pair.symbol,
                        flow_direction = %factor_snapshot.flow_direction,
                        threshold = %flow_spike_threshold,
                        consecutive_cycles = factor_snapshot.consecutive_flow_spike,
                        widen_multiplier = %factor_snapshot.flow_spike_widen_multiplier,
                        "sustained flow spike; widening quotes instead of suppressing"
                    );
                }
            }

            factor_snapshot.volatility += emergency_widening_bps(&self.parsed, state, pair);
            if degraded {
                let utilization = if self.parsed.risk.max_abs_position_usd.is_zero() {
                    Decimal::ZERO
                } else {
                    state.total_abs_position_notional() / self.parsed.risk.max_abs_position_usd
                };
                factor_snapshot.volatility +=
                    proportional_vol_widening(factor_snapshot.raw_volatility, utilization);
            }

            let market = state.market.symbols.get(&pair.symbol);
            let best_bid = market.and_then(|market| market.best_bid);
            let best_ask = market.and_then(|market| market.best_ask);
            let mid = market.and_then(|market| market.mid_price());
            let Some(instrument) = instrument_by_symbol.get(&pair.symbol) else {
                warn!(symbol = %pair.symbol, "missing instrument metadata; skipping symbol");
                self.persist_strategy_snapshot(build_strategy_snapshot(
                    state,
                    &pair.symbol,
                    self.config.runtime.dry_run,
                    "skip",
                    Some("missing_instrument_metadata"),
                    current_position,
                    position_price,
                    has_position,
                    unwind_only,
                    stale_maker_unwind,
                    false,
                    degraded,
                    market,
                    Some(&factor_snapshot),
                    Some(bid_lvl0_multiplier),
                    Some(ask_lvl0_multiplier),
                    0,
                    &[],
                ))?;
                continue;
            };
            let reference_price = mid.or(best_bid).or(best_ask).unwrap_or(Decimal::ZERO);
            let parsed_pair = self
                .parsed
                .pairs
                .iter()
                .find(|parsed_pair| parsed_pair.symbol == pair.symbol)
                .with_context(|| format!("missing parsed pair config for {}", pair.symbol))?;
            // Compute pos_ratio for size asymmetry (S2) before generating quotes.
            let pos_ratio = if parsed_pair.max_position_base.is_zero() {
                Decimal::ZERO
            } else {
                (current_position / parsed_pair.max_position_base)
                    .clamp(-Decimal::ONE, Decimal::ONE)
            };

            let quotes = generate_quotes(
                &self.config.model,
                &self.parsed,
                pair,
                &factor_snapshot,
                fill_tracker,
                best_bid,
                best_ask,
                pos_ratio,
            );
            let generated_quote_count = quotes.len();
            info!(
                symbol = %pair.symbol,
                generated_quote_count,
                unwind_only,
                emergency_unwind = emergency_unwind_symbols.contains(&pair.symbol),
                best_bid = ?best_bid,
                best_ask = ?best_ask,
                reference_price = %reference_price,
                pos_ratio = %pos_ratio,
                "generated raw quotes for desired-order build"
            );

            // Online kappa: record whether a level-0 fill happened this cycle.
            // A level-0 fill means the tracker saw a fill at level_index=0 for this symbol.
            // Proxy: check if a fill was recorded since last cycle using toxicity decay.
            // Simpler: just record the cycle every tick; fill events call record_fill separately.
            // We track was_filled_l0 = toxicity score changed from previous (approximation).
            // For now: always record as not-filled; fill events update via record_fill.
            // The kappa_tracker EWMA blends in the fill signal from record_fill automatically.
            // NOTE: record_quote_cycle is called here but fill_tracker is immutable in this fn.
            // Use a separate mutable pass — moved to the engine run loop (see below).

            // A-S prices from generate_quotes are already final — no tick improvement override.

            let mut remaining_bid_capacity = live_side_capacity_base(
                &self.parsed,
                parsed_pair,
                current_position,
                state.total_abs_position_notional(),
                reference_price,
                Side::Bid,
            );
            let mut remaining_ask_capacity = live_side_capacity_base(
                &self.parsed,
                parsed_pair,
                current_position,
                state.total_abs_position_notional(),
                reference_price,
                Side::Ask,
            );
            // Fix 2/3: is this side an unwind (closing) side given current position?
            // True when selling into a long or buying into a short.
            // Fix 3: in emergency-unwind mode, relax the min-trade-amount floor to the
            // absolute instrument minimum so the position can always be unwound.
            let emergency_unwind = emergency_unwind_symbols.contains(&pair.symbol);
            // Effective min-trade-amount: use 0 in emergency so any sized order passes.
            let effective_min_trade_amount = if emergency_unwind || stale_maker_unwind {
                Decimal::ZERO
            } else {
                min_trade_amount
            };

            let mut symbol_orders = Vec::new();

            for quote in quotes
                .into_iter()
                .filter(|quote| quote.quantity > Decimal::ZERO)
            {
                // Issue 6: post-fill side cooldown — skip this side for 3 s after a toxic fill.

                // Fix 2: determine if this specific quote is on the unwind side.
                let is_unwind = match quote.side {
                    Side::Ask => current_position > Decimal::ZERO,
                    Side::Bid => current_position < Decimal::ZERO,
                };
                let mut quantity = quote.quantity;
                if unwind_only && !is_unwind {
                    quantity *= non_unwind_size_reduction;
                }
                if !is_unwind {
                    if let Some(&cooled_at) = side_cooldown.get(&(pair.symbol.clone(), quote.side))
                    {
                        if cooled_at.elapsed() < Duration::from_secs(3) {
                            info!(
                                symbol = %pair.symbol,
                                side = ?quote.side,
                                level_index = quote.level_index,
                                cooldown_elapsed_ms = cooled_at.elapsed().as_millis(),
                                "filtered quote in desired-order loop: side cooldown active"
                            );
                            continue;
                        }
                    }
                }
                let quote_min_trade_amount = if is_unwind {
                    effective_min_trade_amount
                } else {
                    min_trade_amount
                };

                let mut price =
                    round_to_tick_for_side(quote.price, instrument.tick_size, quote.side);
                if degraded {
                    quantity *= Decimal::new(7, 1);
                }
                // Level 0 anchors to the BBO — skip the index buffer push so the BBO
                // anchor set in distribution.rs is not overridden by a 2-bps min gap.
                // Deeper levels still need the buffer to avoid crossing the index.
                let apply_index_buffer = quote.level_index > 0;
                if apply_index_buffer {
                    if let Some(mid) = mid {
                        price = push_outside_index_buffer(
                            &self.parsed,
                            price,
                            quote.side,
                            mid,
                            instrument.tick_size,
                        );
                    }
                }
                if self.config.model.prevent_spread_crossing {
                    price =
                        clip_to_bbo(price, quote.side, best_bid, best_ask, instrument.tick_size);
                }

                // Fix 2: for the unwind side, floor quantity to min_trade_amount/price
                // so skew-reduced sizes never disappear below the notional floor.
                let effective_quantity = quantize_order_quantity_for_checks(
                    quantity,
                    instrument,
                    self.parsed.model.min_step_volume,
                );
                if effective_quantity <= Decimal::ZERO {
                    info!(
                        symbol = %pair.symbol,
                        side = ?quote.side,
                        level_index = quote.level_index,
                        raw_quantity = %quantity,
                        min_step_volume = %self.parsed.model.min_step_volume,
                        "filtered quote in desired-order loop: effective quantity quantized to zero"
                    );
                    continue;
                }
                if effective_quantity * price < quote_min_trade_amount {
                    if is_unwind && price > Decimal::ZERO {
                        // Floor to the minimum notional; the position MUST be quoted.
                        quantity = (quote_min_trade_amount / price).max(Decimal::new(1, 9));
                        // re-quantize; if still zero the instrument step is too large — skip
                        let floored = quantize_order_quantity_for_checks(
                            quantity,
                            instrument,
                            self.parsed.model.min_step_volume,
                        );
                        if floored <= Decimal::ZERO {
                            continue;
                        }
                        quantity = floored;
                    } else {
                        info!(
                            symbol = %pair.symbol,
                            side = ?quote.side,
                            level_index = quote.level_index,
                            effective_quantity = %effective_quantity,
                            price = %price,
                            notional = %(effective_quantity * price),
                            min_trade_amount = %quote_min_trade_amount,
                            is_unwind,
                            "filtered quote in desired-order loop: below minimum notional"
                        );
                        continue;
                    }
                }

                if !self.config.runtime.dry_run {
                    let remaining_capacity = match quote.side {
                        Side::Bid => &mut remaining_bid_capacity,
                        Side::Ask => &mut remaining_ask_capacity,
                    };
                    if *remaining_capacity <= Decimal::ZERO {
                        info!(
                            symbol = %pair.symbol,
                            side = ?quote.side,
                            level_index = quote.level_index,
                            remaining_capacity = %*remaining_capacity,
                            "filtered quote in desired-order loop: no remaining side capacity"
                        );
                        continue;
                    }
                    if quantity > *remaining_capacity {
                        quantity = *remaining_capacity;
                    }
                    let effective_quantity = quantize_order_quantity_for_checks(
                        quantity,
                        instrument,
                        self.parsed.model.min_step_volume,
                    );
                    if effective_quantity <= Decimal::ZERO
                        || effective_quantity * price < quote_min_trade_amount
                    {
                        // Fix 2: floor rather than skip for unwind side.
                        if is_unwind && price > Decimal::ZERO {
                            let floored_qty = quantize_order_quantity_for_checks(
                                (quote_min_trade_amount / price).max(Decimal::new(1, 9)),
                                instrument,
                                self.parsed.model.min_step_volume,
                            );
                            if floored_qty > Decimal::ZERO && floored_qty <= *remaining_capacity {
                                quantity = floored_qty;
                            } else {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                    *remaining_capacity -= quantity;
                }

                let request = OrderRequest {
                    symbol: quote.symbol,
                    contract_id: instrument.contract_id,
                    level_index: quote.level_index,
                    side: quote.side,
                    order_type: OrderType::Limit,
                    time_in_force: TimeInForce::GoodTillTime,
                    price: Some(price),
                    quantity,
                    post_only: quote.post_only,
                };
                symbol_orders.push(request);
            }

            if symbol_orders.is_empty() && has_position {
                // No orders were generated but we hold a position – place a
                // minimal forced exit order to prevent being stuck indefinitely.
                // This bypasses flow-spike, vol-cut, and cooldown filters.
                let exit_side = if current_position > Decimal::ZERO {
                    Side::Ask
                } else {
                    Side::Bid
                };
                let exit_price = match exit_side {
                    Side::Ask => best_bid,
                    Side::Bid => best_ask,
                };
                if let Some(price) = exit_price {
                    let quantity = instrument.min_order_size.max(current_position.abs());
                    let effective_quantity = quantize_order_quantity_for_checks(
                        quantity,
                        instrument,
                        self.parsed.model.min_step_volume,
                    );
                    let effective_notional = effective_quantity * price;
                    if effective_quantity <= Decimal::ZERO || effective_notional < min_trade_amount
                    {
                        warn!(
                            symbol = %pair.symbol,
                            position = %current_position,
                            exit_side = ?exit_side,
                            price = %price,
                            quantity = %quantity,
                            effective_quantity = %effective_quantity,
                            effective_notional = %effective_notional,
                            min_trade_amount = %min_trade_amount,
                            emergency_unwind,
                            "skipping forced exit order below minimum notional"
                        );
                        continue;
                    }
                    warn!(
                        symbol = %pair.symbol,
                        position = %current_position,
                        exit_side = ?exit_side,
                        price = %price,
                        quantity = %effective_quantity,
                        emergency_unwind,
                        "zero orders generated while holding position; placing forced exit order"
                    );
                    symbol_orders.push(OrderRequest {
                        symbol: pair.symbol.clone(),
                        contract_id: instrument.contract_id,
                        level_index: 0,
                        side: exit_side,
                        order_type: OrderType::Limit,
                        time_in_force: TimeInForce::GoodTillTime,
                        price: Some(price),
                        quantity: effective_quantity,
                        post_only: false,
                    });
                } else {
                    warn!(
                        symbol = %pair.symbol,
                        position = %current_position,
                        emergency_unwind,
                        "zero orders generated for symbol while holding a non-zero position; no BBO available for forced exit"
                    );
                }
            }
            clip_symbol_orders_to_bbo(&mut symbol_orders, best_bid, best_ask, instrument.tick_size);
            let bid_count = symbol_orders
                .iter()
                .filter(|order| order.side == Side::Bid)
                .count();
            let ask_count = symbol_orders
                .iter()
                .filter(|order| order.side == Side::Ask)
                .count();
            let top_bid = symbol_orders
                .iter()
                .filter(|order| order.side == Side::Bid)
                .filter_map(|order| order.price)
                .max();
            let top_ask = symbol_orders
                .iter()
                .filter(|order| order.side == Side::Ask)
                .filter_map(|order| order.price)
                .min();
            let total_bid_qty: Decimal = symbol_orders
                .iter()
                .filter(|order| order.side == Side::Bid)
                .map(|order| order.quantity)
                .sum();
            let total_ask_qty: Decimal = symbol_orders
                .iter()
                .filter(|order| order.side == Side::Ask)
                .map(|order| order.quantity)
                .sum();
            info!(
                symbol = %pair.symbol,
                generated_quote_count,
                desired_order_count = symbol_orders.len(),
                bid_count,
                ask_count,
                total_bid_qty = %total_bid_qty,
                total_ask_qty = %total_ask_qty,
                top_bid = ?top_bid,
                top_ask = ?top_ask,
                unwind_only,
                emergency_unwind,
                "built desired orders for symbol"
            );
            let decision = if !symbol_orders.is_empty() {
                if stale_maker_unwind {
                    "stale_maker_unwind_quoted"
                } else if unwind_only {
                    "unwind_only_quoted"
                } else {
                    "quoted"
                }
            } else if stale_maker_unwind {
                "stale_maker_unwind_suppressed"
            } else if unwind_only {
                "unwind_only_suppressed"
            } else {
                "suppressed"
            };
            self.persist_strategy_snapshot(build_strategy_snapshot(
                state,
                &pair.symbol,
                self.config.runtime.dry_run,
                decision,
                None,
                current_position,
                position_price,
                has_position,
                unwind_only,
                stale_maker_unwind,
                emergency_unwind,
                degraded,
                market,
                Some(&factor_snapshot),
                Some(bid_lvl0_multiplier),
                Some(ask_lvl0_multiplier),
                generated_quote_count,
                &symbol_orders,
            ))?;
            orders.extend(symbol_orders.iter().cloned());
        }

        info!(
            desired_order_count = orders.len(),
            enabled_pairs = self.config.pairs.iter().filter(|pair| pair.enabled).count(),
            "build_desired_orders completed"
        );
        Ok(orders)
    }
}

fn build_strategy_snapshot(
    state: &BotState,
    symbol: &str,
    is_simulated: bool,
    decision: &str,
    skip_reason: Option<&str>,
    current_position: Decimal,
    position_price: Decimal,
    has_position: bool,
    unwind_only: bool,
    stale_maker_unwind: bool,
    emergency_unwind: bool,
    degraded: bool,
    market: Option<&crate::core::state::SymbolMarketState>,
    factor_snapshot: Option<&crate::domain::FactorSnapshot>,
    bid_lvl0_multiplier: Option<Decimal>,
    ask_lvl0_multiplier: Option<Decimal>,
    generated_quote_count: usize,
    orders: &[OrderRequest],
) -> StrategySnapshot {
    let bid_count = orders
        .iter()
        .filter(|order| order.side == Side::Bid)
        .count() as i64;
    let ask_count = orders
        .iter()
        .filter(|order| order.side == Side::Ask)
        .count() as i64;
    let total_bid_qty: Decimal = orders
        .iter()
        .filter(|order| order.side == Side::Bid)
        .map(|order| order.quantity)
        .sum();
    let total_ask_qty: Decimal = orders
        .iter()
        .filter(|order| order.side == Side::Ask)
        .map(|order| order.quantity)
        .sum();
    let top_bid = orders
        .iter()
        .filter(|order| order.side == Side::Bid)
        .filter_map(|order| order.price)
        .max();
    let top_ask = orders
        .iter()
        .filter(|order| order.side == Side::Ask)
        .filter_map(|order| order.price)
        .min();
    let position_notional = current_position.abs() * position_price;

    StrategySnapshot {
        ts: Utc::now(),
        symbol: symbol.to_string(),
        is_simulated,
        decision: decision.to_string(),
        skip_reason: skip_reason.map(str::to_string),
        current_position,
        position_price,
        position_notional,
        has_position,
        unwind_only,
        stale_maker_unwind,
        emergency_unwind,
        degraded,
        best_bid: market.and_then(|m| m.best_bid),
        best_ask: market.and_then(|m| m.best_ask),
        mid_price: market.and_then(|m| m.mid_price()),
        mark_price: market.and_then(|m| m.mark_price),
        spot_price: market.and_then(|m| m.spot_price),
        bbo_spread_bps: market.and_then(|m| m.bbo_spread_bps()),
        bbo_bid_size: market.and_then(|m| m.bbo_bid_size),
        bbo_ask_size: market.and_then(|m| m.bbo_ask_size),
        price_index: factor_snapshot.map(|f| f.price_index),
        raw_volatility: factor_snapshot.map(|f| f.raw_volatility),
        volatility: factor_snapshot.map(|f| f.volatility),
        inventory_lean_bps: factor_snapshot.map(|f| f.inventory_lean_bps),
        volume_imbalance: factor_snapshot.map(|f| f.volume_imbalance),
        flow_direction: factor_snapshot.map(|f| f.flow_direction),
        inventory_skew: factor_snapshot.map(|f| f.inventory_skew),
        recent_trade_count: factor_snapshot.map(|f| f.recent_trade_count as i64),
        regime: factor_snapshot.map(|f| format!("{:?}", f.regime)),
        regime_intensity: factor_snapshot.map(|f| f.regime_intensity),
        ob_imbalance: factor_snapshot.map(|f| f.ob_imbalance),
        consecutive_flow_spike: factor_snapshot.map(|f| i64::from(f.consecutive_flow_spike)),
        microprice: factor_snapshot.map(|f| f.microprice),
        fill_rate_skew: factor_snapshot.map(|f| f.fill_rate_skew),
        vpin: factor_snapshot.map(|f| f.vpin),
        funding_lean: factor_snapshot.map(|f| f.funding_lean),
        kappa_estimate: factor_snapshot.and_then(|f| f.kappa_estimate),
        post_fill_widen_bid: factor_snapshot.map(|f| f.post_fill_widen_bid),
        post_fill_widen_ask: factor_snapshot.map(|f| f.post_fill_widen_ask),
        flow_spike_widen_multiplier: factor_snapshot.map(|f| f.flow_spike_widen_multiplier),
        toxic_regime_persistence_secs: factor_snapshot
            .map(|f| f.toxic_regime_persistence_secs as i64),
        bid_lvl0_multiplier,
        ask_lvl0_multiplier,
        generated_quote_count: generated_quote_count as i64,
        desired_order_count: orders.len() as i64,
        bid_count,
        ask_count,
        total_bid_qty,
        total_ask_qty,
        top_bid,
        top_ask,
        account_equity: state.account_equity,
        total_pnl: state.effective_total_pnl(),
    }
}

fn round_to_tick_for_side(price: Decimal, tick_size: Option<Decimal>, side: Side) -> Decimal {
    let Some(tick) = tick_size else {
        return price.round_dp(6);
    };
    if tick.is_zero() {
        return price.round_dp(6);
    }
    let scaled = price / tick;
    match side {
        Side::Bid => scaled.floor() * tick,
        Side::Ask => scaled.ceil() * tick,
    }
}

fn quantize_order_quantity_for_checks(
    quantity: Decimal,
    instrument: &InstrumentMeta,
    config_min_step: Decimal,
) -> Decimal {
    if quantity <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    let decimal_step = Decimal::new(1, instrument.underlying_decimals.min(28));
    let step = instrument
        .min_order_size
        .max(decimal_step)
        .max(config_min_step);
    if step <= Decimal::ZERO {
        return quantity;
    }

    let aligned = (quantity / step).floor() * step;
    if aligned > Decimal::ZERO && aligned < step {
        step.normalize()
    } else {
        aligned.normalize()
    }
}

fn market_price_for_position(state: &BotState, position: &crate::domain::Position) -> Decimal {
    position_price_for_symbol(state, &position.symbol, position.entry_price)
}

fn position_price_for_symbol(state: &BotState, symbol: &str, fallback_price: Decimal) -> Decimal {
    state
        .market
        .symbols
        .get(symbol)
        .and_then(|market| market.mark_price.or_else(|| market.mid_price()))
        .or_else(|| (fallback_price > Decimal::ZERO).then_some(fallback_price))
        .unwrap_or(Decimal::ZERO)
}

fn position_is_effectively_flat(
    quantity: Decimal,
    reference_price: Decimal,
    min_trade_amount: Decimal,
) -> bool {
    if quantity == Decimal::ZERO {
        return true;
    }
    if reference_price <= Decimal::ZERO {
        return false;
    }
    quantity.abs() * reference_price < min_trade_amount
}

fn clip_to_bbo(
    price: Decimal,
    side: Side,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    tick_size: Option<Decimal>,
) -> Decimal {
    match side {
        Side::Bid => {
            if let Some(ask) = best_ask {
                let clipped = (ask - tick_size.unwrap_or(Decimal::new(1, 6))).max(Decimal::ZERO);
                return price.min(clipped);
            }
        }
        Side::Ask => {
            if let Some(bid) = best_bid {
                return price.max(bid + tick_size.unwrap_or(Decimal::new(1, 6)));
            }
        }
    }
    price
}

fn clip_symbol_orders_to_bbo(
    orders: &mut [OrderRequest],
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    tick_size: Option<Decimal>,
) {
    for order in orders.iter_mut() {
        if let Some(price) = order.price {
            order.price = Some(clip_to_bbo(
                price, order.side, best_bid, best_ask, tick_size,
            ));
        }
    }

    let top_bid_index = orders
        .iter()
        .enumerate()
        .filter(|(_, order)| order.side == Side::Bid)
        .filter_map(|(idx, order)| order.price.map(|price| (idx, price)))
        .max_by(|left, right| left.1.cmp(&right.1))
        .map(|(idx, _)| idx);
    let top_ask_index = orders
        .iter()
        .enumerate()
        .filter(|(_, order)| order.side == Side::Ask)
        .filter_map(|(idx, order)| order.price.map(|price| (idx, price)))
        .min_by(|left, right| left.1.cmp(&right.1))
        .map(|(idx, _)| idx);

    let Some(bid_idx) = top_bid_index else {
        return;
    };
    let Some(ask_idx) = top_ask_index else {
        return;
    };
    let Some(mut bid_price) = orders[bid_idx].price else {
        return;
    };
    let Some(mut ask_price) = orders[ask_idx].price else {
        return;
    };
    let tick = tick_size.unwrap_or(Decimal::new(1, 6));
    if bid_price >= ask_price {
        bid_price = (ask_price - tick).max(Decimal::ZERO);
        ask_price = ask_price.max(bid_price + tick);
        orders[bid_idx].price = Some(bid_price);
        orders[ask_idx].price = Some(ask_price);
    }
}

fn bbo_gap_bps(order: &crate::domain::OpenOrder, state: &BotState) -> Option<Decimal> {
    let market = state.market.symbols.get(&order.symbol)?;
    let order_price = order.price?;
    let reference = match order.side {
        Side::Bid => market.best_bid?,
        Side::Ask => market.best_ask?,
    };
    if reference <= Decimal::ZERO {
        return None;
    }
    Some(((reference - order_price).abs() / reference) * Decimal::from(10_000u64))
}

fn should_cancel_on_adverse_bbo_move(
    current_orders: &HashMap<String, crate::domain::OpenOrder>,
    state: &BotState,
) -> bool {
    current_orders.values().any(|order| {
        bbo_gap_bps(order, state)
            .map(|gap| gap > Decimal::from(2u64))
            .unwrap_or(false)
    })
}

fn filter_orders_against_current_bbo(
    parsed: &ParsedConfig,
    state: &BotState,
    orders: &[OrderRequest],
) -> Vec<OrderRequest> {
    orders
        .iter()
        .filter_map(|order| {
            let Some(price) = order.price else {
                return Some(order.clone());
            };
            if order.time_in_force == TimeInForce::ImmediateOrCancel {
                return Some(order.clone());
            }
            let market = state.market.symbols.get(&order.symbol)?;
            let same_side_bbo = match order.side {
                Side::Bid => market.best_bid,
                Side::Ask => market.best_ask,
            };
            let opposite_bbo = match order.side {
                Side::Bid => market.best_ask,
                Side::Ask => market.best_bid,
            };
            if let Some(opposite) = opposite_bbo {
                let crosses = match order.side {
                    Side::Bid => price >= opposite,
                    Side::Ask => price <= opposite,
                };
                if crosses {
                    return None;
                }
            }
            if order.level_index == 0 {
                if let Some(reference) = same_side_bbo {
                    if reference > Decimal::ZERO {
                        let max_gap_bps = parsed.model.born_inf_bps + Decimal::from(2u64);
                        let gap_bps =
                            ((reference - price).abs() / reference) * Decimal::from(10_000u64);
                        if gap_bps > max_gap_bps {
                            return None;
                        }
                    }
                }
            }
            Some(order.clone())
        })
        .collect()
}

fn push_outside_index_buffer(
    parsed: &ParsedConfig,
    price: Decimal,
    side: Side,
    mid: Decimal,
    tick_size: Option<Decimal>,
) -> Decimal {
    let buffer = parsed.model.index_forward_buffer_bps / Decimal::from(10_000u64);
    match side {
        Side::Bid => {
            let max_bid = mid * (Decimal::ONE - buffer);
            round_to_tick_for_side(price.min(max_bid), tick_size, side)
        }
        Side::Ask => {
            let min_ask = mid * (Decimal::ONE + buffer);
            round_to_tick_for_side(price.max(min_ask), tick_size, side)
        }
    }
}

fn crosses_index_with_buffer(
    parsed: &ParsedConfig,
    price: Decimal,
    side: Side,
    mid: Decimal,
) -> bool {
    let buffer = parsed.model.index_forward_buffer_bps / Decimal::from(10_000u64);
    match side {
        Side::Bid => price >= mid * (Decimal::ONE - buffer),
        Side::Ask => price <= mid * (Decimal::ONE + buffer),
    }
}

fn build_reconciliation_plan(
    parsed: &ParsedConfig,
    state: &BotState,
    current_orders_source: &HashMap<String, crate::domain::OpenOrder>,
    desired: &[OrderRequest],
) -> Result<ReconciliationPlan> {
    let price_floor = parsed.model.min_step_price;
    let volume_floor = parsed.model.min_step_volume;
    let price_threshold = parsed.model.price_sensitivity_threshold;
    let price_scaling = parsed.model.price_sensitivity_scaling_factor;
    let volume_threshold = parsed.model.volume_sensitivity_threshold;

    // fills_since_recon suppression removed: the fill_tracker toxicity score already
    // reduces size at adversely-selected levels, and suppressing by (symbol, side, level)
    // prevented legitimate replacement orders after fills.
    let desired: Vec<OrderRequest> = desired.to_vec();

    let mut changed_orders = 0;
    let mut to_cancel_order_ids = Vec::new();
    let mut to_place = Vec::new();
    let mut result = ReconResult::default();

    let mut desired_by_symbol_side: HashMap<(String, Side), Vec<&OrderRequest>> = HashMap::new();
    for order in &desired {
        desired_by_symbol_side
            .entry((order.symbol.clone(), order.side))
            .or_default()
            .push(order);
    }
    for orders in desired_by_symbol_side.values_mut() {
        orders.sort_by_key(|order| order.level_index);
    }

    let mut current_by_symbol_side: HashMap<(String, Side), Vec<&crate::domain::OpenOrder>> =
        HashMap::new();
    for order in current_orders_source.values() {
        current_by_symbol_side
            .entry((order.symbol.clone(), order.side))
            .or_default()
            .push(order);
    }
    for orders in current_by_symbol_side.values_mut() {
        orders.sort_by_key(|order| order.level_index.unwrap_or(usize::MAX));
    }

    let crossing_mid_exists = current_orders_source.values().any(|order| {
        let Some(market) = state.market.symbols.get(&order.symbol) else {
            return false;
        };
        let Some(mid) = market.mid_price() else {
            return false;
        };
        let Some(price) = order.price else {
            return false;
        };
        crosses_index_with_buffer(parsed, price, order.side, mid)
    });

    for (key, target_orders) in &desired_by_symbol_side {
        let current_orders = current_by_symbol_side.get(key).cloned().unwrap_or_default();
        let diff = diff_group(
            current_orders.as_slice(),
            target_orders.as_slice(),
            price_floor,
            volume_floor,
            price_threshold,
            price_scaling,
            volume_threshold,
        );
        changed_orders += diff.changed_count;
        result.unexpected_orders += diff.unexpected_orders;
        result.missing_orders += diff.missing_orders;
        to_cancel_order_ids.extend(diff.to_cancel_order_ids);
        to_place.extend(diff.to_place);
    }

    for (key, current_orders) in &current_by_symbol_side {
        if !desired_by_symbol_side.contains_key(key) {
            changed_orders += current_orders.len();
            result.unexpected_orders += current_orders.len();
            to_cancel_order_ids.extend(
                current_orders
                    .iter()
                    .filter_map(|order| order.order_id.clone()),
            );
        }
    }

    if crossing_mid_exists {
        for order in current_orders_source.values() {
            let Some(market) = state.market.symbols.get(&order.symbol) else {
                continue;
            };
            let Some(mid) = market.mid_price() else {
                continue;
            };
            let Some(price) = order.price else {
                continue;
            };
            if crosses_index_with_buffer(parsed, price, order.side, mid) {
                if let Some(order_id) = order.order_id.clone() {
                    to_cancel_order_ids.push(order_id);
                }
            }
        }
    }
    to_cancel_order_ids.sort_unstable();
    to_cancel_order_ids.dedup();

    Ok(ReconciliationPlan {
        to_cancel_order_ids,
        to_place,
        changed_orders,
        result,
    })
}

fn log_dry_run_inventory(state: &BotState) {
    for position in state
        .positions
        .values()
        .filter(|position| !position.quantity.is_zero())
    {
        info!(
            symbol = %position.symbol,
            quantity = %position.quantity,
            entry_price = %position.entry_price,
            realized_pnl = %position.realized_pnl,
            unrealized_pnl = %position.unrealized_pnl,
            account_equity = %state.account_equity,
            startup_account_equity = ?state.startup_account_equity,
            session_account_pnl = ?state.session_account_pnl(),
            total_fees = %state.pnl.total_fees,
            total_pnl = %state.effective_total_pnl(),
            "dry-run inventory"
        );
    }
}

fn log_minute_stats(config: &AppConfig, state: &BotState, stats: &MinuteStats) {
    let average_position = if stats.position_sample_count == 0 {
        Decimal::ZERO
    } else {
        stats.signed_position_sum / Decimal::from(stats.position_sample_count as u64)
    };
    let realized_pnl: Decimal = config
        .pairs
        .iter()
        .filter(|pair| pair.enabled)
        .map(|pair| {
            state
                .positions
                .get(&pair.symbol)
                .map(|position| position.realized_pnl)
                .unwrap_or(Decimal::ZERO)
        })
        .sum();
    let unrealized_pnl: Decimal = config
        .pairs
        .iter()
        .filter(|pair| pair.enabled)
        .map(|pair| {
            state
                .positions
                .get(&pair.symbol)
                .map(|position| position.unrealized_pnl)
                .unwrap_or(Decimal::ZERO)
        })
        .sum();

    // Notional-weighted average spread earned across all live fills this minute.
    let live_fills = stats
        .fills_count
        .saturating_sub(stats.simulated_fills_count);
    let avg_spread_earned_bps = if stats.fill_notional_sum > Decimal::ZERO {
        (stats.spread_earned_bps_sum / stats.fill_notional_sum).round_dp(2)
    } else {
        Decimal::ZERO
    };
    // Toxic fill ratio: fraction of fills where adverse selection exceeded spread.
    let toxic_fill_ratio = if live_fills > 0 {
        Decimal::from(stats.toxic_fill_count) / Decimal::from(live_fills as u64)
    } else {
        Decimal::ZERO
    };
    // Simple profitability signal: positive when spread > adverse selection on average.
    let profitability_signal = if live_fills > 0 {
        // avg_spread_earned_bps > 1 and low toxicity = healthy; < 1 or high toxicity = bad.
        avg_spread_earned_bps - toxic_fill_ratio * Decimal::from(10u64)
    } else {
        Decimal::ZERO
    };
    let session_account_pnl = state.session_account_pnl();

    info!(
        trade_count = stats.trade_count,
        fills_count = stats.fills_count,
        simulated_fills_count = stats.simulated_fills_count,
        live_fills_count = live_fills,
        missed_crosses_count = stats.missed_crosses_count,
        near_misses_count = stats.near_misses_count,
        average_position = %average_position,
        realized_pnl = %realized_pnl,
        unrealized_pnl = %unrealized_pnl,
        account_equity = %state.account_equity,
        startup_account_equity = ?state.startup_account_equity,
        session_account_pnl = ?session_account_pnl,
        total_fees = %state.pnl.total_fees,
        total_pnl = %state.effective_total_pnl(),
        avg_spread_earned_bps = %avg_spread_earned_bps,
        toxic_fill_ratio = %toxic_fill_ratio,
        toxic_fill_count = stats.toxic_fill_count,
        profitability_signal = %profitability_signal,
        "minute stats"
    );
}

fn log_missed_fill_opportunity(
    state: &BotState,
    symbol: &str,
    trade_price: Decimal,
    trade_quantity: Decimal,
    taker_side: Option<Side>,
    timestamp: chrono::DateTime<Utc>,
    simulated_fills: &[Fill],
) -> (bool, bool) {
    if !simulated_fills.is_empty() {
        return (false, false);
    }

    let top_bid = state
        .dry_run_orders
        .values()
        .filter(|order| order.symbol == symbol && order.side == Side::Bid)
        .filter_map(|order| order.price)
        .max();
    let top_ask = state
        .dry_run_orders
        .values()
        .filter(|order| order.symbol == symbol && order.side == Side::Ask)
        .filter_map(|order| order.price)
        .min();

    let crossed_bid = top_bid.map(|bid| trade_price <= bid).unwrap_or(false);
    let crossed_ask = top_ask.map(|ask| trade_price >= ask).unwrap_or(false);

    let one_bp = Decimal::new(1, 0);
    let near_bid_bps = top_bid.and_then(|bid| {
        if bid.is_zero() {
            None
        } else {
            Some(((bid - trade_price).abs() / bid) * Decimal::from(10_000u64))
        }
    });
    let near_ask_bps = top_ask.and_then(|ask| {
        if ask.is_zero() {
            None
        } else {
            Some(((trade_price - ask).abs() / ask) * Decimal::from(10_000u64))
        }
    });

    if crossed_bid || crossed_ask {
        warn!(
            symbol = %symbol,
            trade_price = %trade_price,
            trade_quantity = %trade_quantity,
            taker_side = ?taker_side,
            timestamp = %timestamp,
            top_bid = ?top_bid,
            top_ask = ?top_ask,
            crossed_bid,
            crossed_ask,
            "trade crossed resting dry-run quote without simulated fill"
        );
        (true, false)
    } else if near_bid_bps.map(|bps| bps <= one_bp).unwrap_or(false)
        || near_ask_bps.map(|bps| bps <= one_bp).unwrap_or(false)
    {
        (false, true)
    } else {
        (false, false)
    }
}

fn should_flatten_on_breaker(message: &str) -> bool {
    message.contains("drawdown limit breached")
        || message.contains("global position limit breached")
        || message.contains("symbol position limit breached")
        || message.contains("correlated position limit breached")
}

fn emergency_widening_bps(
    parsed: &ParsedConfig,
    state: &BotState,
    pair: &crate::config::PairConfig,
) -> Decimal {
    if parsed.risk.max_symbol_position_usd.is_zero() || parsed.risk.emergency_widening_bps.is_zero()
    {
        return Decimal::ZERO;
    }

    let Some(position) = state.positions.get(&pair.symbol) else {
        return Decimal::ZERO;
    };
    let Some(market) = state.market.symbols.get(&pair.symbol) else {
        return Decimal::ZERO;
    };
    let Some(reference_price) = market.mark_price.or_else(|| market.mid_price()) else {
        return Decimal::ZERO;
    };

    let utilization = ((position.quantity * reference_price).abs()
        / parsed.risk.max_symbol_position_usd)
        .min(Decimal::ONE);
    if utilization < parsed.risk.emergency_skew_start {
        return Decimal::ZERO;
    }
    parsed.risk.emergency_widening_bps
        * utilization
        * parsed.risk.emergency_skew_max.max(Decimal::ONE)
}

fn live_side_capacity_base(
    parsed: &ParsedConfig,
    pair: &crate::config::ParsedPairConfig,
    current_position: Decimal,
    total_abs_position_notional: Decimal,
    reference_price: Decimal,
    side: Side,
) -> Decimal {
    // Fix 2: the unwind (close) side reduces existing exposure, so USD position
    // limits must NOT block it — applying them here causes the "death spiral"
    // where a full position has zero ask capacity and can never unwind.
    let is_unwind = match side {
        Side::Ask => current_position > Decimal::ZERO, // long → selling reduces position
        Side::Bid => current_position < Decimal::ZERO, // short → buying reduces position
    };
    if is_unwind {
        // Can close at most abs(position), bounded by max_position_base.
        return current_position
            .abs()
            .min(pair.max_position_base)
            .max(Decimal::ZERO);
    }

    // Accumulation side: apply the full USD + pair position limits.
    let pair_capacity = match side {
        Side::Bid => {
            (pair.max_position_base - current_position.max(Decimal::ZERO)).max(Decimal::ZERO)
        }
        Side::Ask => {
            (pair.max_position_base - (-current_position).max(Decimal::ZERO)).max(Decimal::ZERO)
        }
    };

    if reference_price <= Decimal::ZERO {
        return pair_capacity;
    }

    let symbol_position_notional = (current_position * reference_price).abs();
    let symbol_usd_capacity = (parsed.risk.max_symbol_position_usd - symbol_position_notional)
        .max(Decimal::ZERO)
        / reference_price;
    let global_usd_capacity = (parsed.risk.max_abs_position_usd - total_abs_position_notional)
        .max(Decimal::ZERO)
        / reference_price;

    pair_capacity
        .min(symbol_usd_capacity.max(Decimal::ZERO))
        .min(global_usd_capacity.max(Decimal::ZERO))
        .max(Decimal::ZERO)
}

fn diff_group(
    current: &[&crate::domain::OpenOrder],
    target: &[&OrderRequest],
    price_floor: Decimal,
    volume_floor: Decimal,
    price_threshold: Decimal,
    price_scaling: Decimal,
    volume_threshold: Decimal,
) -> GroupDiff {
    let mut diff = GroupDiff::default();

    let current_by_level: HashMap<usize, &crate::domain::OpenOrder> = current
        .iter()
        .filter_map(|order| order.level_index.map(|level_index| (level_index, *order)))
        .collect();
    let target_by_level: HashMap<usize, &OrderRequest> = target
        .iter()
        .map(|order| (order.level_index, *order))
        .collect();

    for current_order in current.iter().filter(|order| order.level_index.is_none()) {
        diff.changed_count += 1;
        diff.unexpected_orders += 1;
        if let Some(order_id) = current_order.order_id.clone() {
            diff.to_cancel_order_ids.push(order_id);
        }
    }

    let mut all_levels: Vec<usize> = current_by_level
        .keys()
        .chain(target_by_level.keys())
        .copied()
        .collect();
    all_levels.sort_unstable();
    all_levels.dedup();

    for level_index in all_levels {
        let current_order = current_by_level.get(&level_index).copied();
        let target_order = target_by_level.get(&level_index).copied();
        match (current_order, target_order) {
            (Some(current_order), Some(target_order)) => {
                let level_scale = Decimal::ONE + price_scaling * Decimal::from(level_index as u64);
                let current_price = current_order.price.unwrap_or(Decimal::ZERO);
                let target_price = target_order.price.unwrap_or(Decimal::ZERO);
                let price_delta = (current_price - target_price).abs();
                let volume_delta = (current_order.remaining_quantity - target_order.quantity).abs();

                let price_limit =
                    price_floor.max(target_price.abs() * price_threshold) * level_scale;
                let volume_limit = volume_floor.max(target_order.quantity.abs() * volume_threshold);

                if price_delta > price_limit || volume_delta > volume_limit {
                    diff.changed_count += 1;
                    if let Some(order_id) = current_order.order_id.clone() {
                        diff.to_cancel_order_ids.push(order_id);
                    }
                    diff.to_place.push(target_order.clone());
                }
            }
            (Some(current_order), None) => {
                diff.changed_count += 1;
                diff.unexpected_orders += 1;
                if let Some(order_id) = current_order.order_id.clone() {
                    diff.to_cancel_order_ids.push(order_id);
                }
            }
            (None, Some(target_order)) => {
                diff.changed_count += 1;
                diff.missing_orders += 1;
                diff.to_place.push(target_order.clone());
            }
            (None, None) => {}
        }
    }

    diff
}

fn infer_fill_analytics(state: &BotState, fill: &Fill) -> (usize, Decimal) {
    let matching_orders: Vec<&crate::domain::OpenOrder> = state
        .open_orders
        .values()
        .chain(state.dry_run_orders.values())
        .filter(|order| order.symbol == fill.symbol && order.side == fill.side)
        .collect();

    let exact_match = matching_orders.iter().find(|order| {
        fill.order_id
            .as_ref()
            .zip(order.order_id.as_ref())
            .map(|(fill_order_id, order_id)| fill_order_id == order_id)
            .unwrap_or(false)
            || fill
                .nonce
                .map(|fill_nonce| fill_nonce == order.nonce)
                .unwrap_or(false)
    });

    let level_index = exact_match
        .and_then(|order| order.level_index)
        .or_else(|| {
            matching_orders
                .iter()
                .min_by(|left, right| {
                    let left_delta = (left.price.unwrap_or(fill.price) - fill.price).abs();
                    let right_delta = (right.price.unwrap_or(fill.price) - fill.price).abs();
                    left_delta.cmp(&right_delta).then_with(|| {
                        left.level_index
                            .unwrap_or(usize::MAX)
                            .cmp(&right.level_index.unwrap_or(usize::MAX))
                    })
                })
                .and_then(|order| order.level_index)
        })
        .unwrap_or(0);

    // Use mid price as the reference for spread_earned so the denominator
    // reflects our actual edge from fair value (e.g. 5-20 bps), not the full
    // bid-ask spread (which can be 100+ bps on wide markets like GRVT perps).
    // With the old opposite-BBO reference, even a 50 bps adverse move gave
    // toxicity_ratio < 1 when the spread was 80+ bps — so is_toxic was always
    // false and side_cooldown could never trigger.
    let spread_earned_bps = if let Some(market) = state.market.symbols.get(&fill.symbol) {
        let mid = market.mid_price().filter(|&m| m > Decimal::ZERO);
        if let Some(mid) = mid {
            let edge = match fill.side {
                Side::Bid => mid - fill.price, // how far below mid we bought
                Side::Ask => fill.price - mid, // how far above mid we sold
            };
            (edge / mid * Decimal::from(10_000u64)).max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        }
    } else {
        Decimal::ZERO
    };

    (level_index, spread_earned_bps)
}

fn market_reference(
    state: &BotState,
    event: &MarketEvent,
) -> Option<(String, Decimal, chrono::DateTime<Utc>)> {
    match event {
        MarketEvent::MarkPrice {
            symbol,
            price,
            timestamp,
        }
        | MarketEvent::SpotPrice {
            symbol,
            price,
            timestamp,
        } => Some((symbol.clone(), *price, *timestamp)),
        MarketEvent::BestBidAsk {
            symbol,
            bid,
            ask,
            timestamp,
            ..
        } => Some((
            symbol.clone(),
            (*bid + *ask) / Decimal::from(2u64),
            *timestamp,
        )),
        MarketEvent::Trade {
            symbol,
            price,
            timestamp,
            ..
        } => Some((symbol.clone(), *price, *timestamp)),
        MarketEvent::OrderBookSnapshot {
            symbol,
            bids,
            asks,
            timestamp,
        }
        | MarketEvent::OrderBookUpdate {
            symbol,
            bids,
            asks,
            timestamp,
        } => {
            let best_bid = bids.keys().next_back().copied().or_else(|| {
                state
                    .market
                    .symbols
                    .get(symbol)
                    .and_then(|market| market.best_bid)
            });
            let best_ask = asks.keys().next().copied().or_else(|| {
                state
                    .market
                    .symbols
                    .get(symbol)
                    .and_then(|market| market.best_ask)
            });
            match (best_bid, best_ask) {
                (Some(bid), Some(ask)) => Some((
                    symbol.clone(),
                    (bid + ask) / Decimal::from(2u64),
                    *timestamp,
                )),
                _ => None,
            }
        }
        MarketEvent::StreamReconnected { .. } | MarketEvent::FundingRate { .. } => None,
    }
}

fn log_dry_run_orders(_desired: &[OrderRequest]) {}
