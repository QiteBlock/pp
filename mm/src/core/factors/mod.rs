use std::collections::{HashMap, VecDeque};

use crate::{
    config::{AppConfig, PairConfig, ParsedConfig, PriceSource},
    core::{analytics::tracker::FillTracker, state::BotState},
    domain::{FactorSnapshot, MarketRegime, RecentTrade},
};
use chrono::{DateTime, Duration, Timelike, Utc};
use rust_decimal::{prelude::ToPrimitive, Decimal, MathematicalOps};
use tracing::info;

// ─── VPIN tracker ────────────────────────────────────────────────────────────

/// Volume-synchronised probability of informed trading (VPIN).
/// Accumulates trades into equal-volume buckets; each bucket's buy/sell
/// imbalance is the informed-trading estimate for that volume epoch.
struct VpinTracker {
    bucket_size: Decimal,
    current_buy: Decimal,
    current_sell: Decimal,
    buckets: VecDeque<Decimal>, // |imbalance| per completed bucket
    n_buckets: usize,
}

impl VpinTracker {
    fn new(bucket_size: Decimal, n_buckets: usize) -> Self {
        Self {
            bucket_size,
            current_buy: Decimal::ZERO,
            current_sell: Decimal::ZERO,
            buckets: VecDeque::new(),
            n_buckets,
        }
    }

    fn add_trade(&mut self, buy_vol: Decimal, sell_vol: Decimal) {
        if self.bucket_size <= Decimal::ZERO {
            return;
        }
        self.current_buy += buy_vol;
        self.current_sell += sell_vol;
        loop {
            let bucket_total = self.current_buy + self.current_sell;
            if bucket_total < self.bucket_size || bucket_total.is_zero() {
                break;
            }
            let imbalance = ((self.current_buy - self.current_sell) / bucket_total).abs();
            self.buckets.push_back(imbalance);
            while self.buckets.len() > self.n_buckets {
                self.buckets.pop_front();
            }
            // consume one bucket worth
            let frac = self.bucket_size / bucket_total;
            self.current_buy -= self.current_buy * frac;
            self.current_sell -= self.current_sell * frac;
            // clamp to zero to avoid floating-point drift
            if self.current_buy < Decimal::ZERO {
                self.current_buy = Decimal::ZERO;
            }
            if self.current_sell < Decimal::ZERO {
                self.current_sell = Decimal::ZERO;
            }
        }
    }

    fn vpin(&self) -> Decimal {
        if self.buckets.is_empty() {
            return Decimal::ZERO;
        }
        let sum: Decimal = self.buckets.iter().sum();
        sum / Decimal::from(self.buckets.len() as u64)
    }
}

#[derive(Default)]
pub struct FactorEngine {
    symbol_state: HashMap<String, SymbolFactorState>,
    vpin_trackers: HashMap<String, VpinTracker>,
}

struct SymbolFactorState {
    ewma_mean: Option<Decimal>,
    ewma_variance: Decimal,
    last_mid: Option<Decimal>,
    last_mid_timestamp: Option<DateTime<Utc>>,
    vol_history: VecDeque<Decimal>,
    smoothed_flow: Decimal,
    regime: MarketRegime,
    /// Continuous [0, 1] intensity of current regime (S5).
    regime_intensity: Decimal,
    /// When the current regime was entered (S5 dwell time).
    regime_entered_at: Option<DateTime<Utc>>,
    /// Rolling 600s toxicity window sampled once per factor cycle.
    toxic_history: VecDeque<(DateTime<Utc>, bool)>,
    spread_floor: Decimal,
    trade_velocity_ewma: Decimal,
    bbo_spread_ewma: Option<Decimal>,
    last_vpin_trade_time: Option<DateTime<Utc>>,
    last_vpin_trade_count_at_time: usize,
    /// Consecutive factor cycles where |raw_flow_direction| > 0.7.
    /// Used as a fallback to force TrendingToxic even when EWMA lags.
    consecutive_high_raw_flow: u32,
    /// Consecutive factor cycles where |smoothed flow_direction| > flow_spike_pause_threshold.
    /// Used to require a stable spike before fully suppressing quotes.
    consecutive_flow_spike: u32,
}

impl Default for SymbolFactorState {
    fn default() -> Self {
        Self {
            ewma_mean: None,
            ewma_variance: Decimal::ZERO,
            last_mid: None,
            last_mid_timestamp: None,
            vol_history: VecDeque::new(),
            smoothed_flow: Decimal::ZERO,
            regime: MarketRegime::Quiet,
            regime_intensity: Decimal::ZERO,
            regime_entered_at: Some(Utc::now()),
            toxic_history: VecDeque::new(),
            spread_floor: Decimal::ZERO,
            trade_velocity_ewma: Decimal::ZERO,
            bbo_spread_ewma: None,
            last_vpin_trade_time: None,
            last_vpin_trade_count_at_time: 0,
            consecutive_high_raw_flow: 0,
            consecutive_flow_spike: 0,
        }
    }
}

impl FactorEngine {
    pub fn compute(
        &mut self,
        config: &AppConfig,
        parsed: &ParsedConfig,
        pair: &PairConfig,
        state: &BotState,
        fill_tracker: &FillTracker,
    ) -> Option<FactorSnapshot> {
        let Some(market) = state.market.symbols.get(&pair.symbol) else {
            info!(
                symbol = %pair.symbol,
                known_market_symbols = state.market.symbols.len(),
                "compute() returned None: missing market symbol state"
            );
            return None;
        };
        // Use microprice (BBO size-weighted mid) when available, blended by config weight.
        let Some(raw_mid) = market.mid_price() else {
            info!(
                symbol = %pair.symbol,
                best_bid = ?market.best_bid,
                best_ask = ?market.best_ask,
                mark_price = ?market.mark_price,
                spot_price = ?market.spot_price,
                "compute() returned None: market.mid_price() unavailable"
            );
            return None;
        };
        let mid = if let Some(mp) = market.microprice() {
            let w = parsed.factors.microprice_weight;
            w * mp + (Decimal::ONE - w) * raw_mid
        } else {
            raw_mid
        };
        let Some(now) = market.last_updated else {
            info!(
                symbol = %pair.symbol,
                market_last_updated_at = ?state.market.last_updated_at,
                "compute() returned None: market.last_updated unavailable"
            );
            return None;
        };
        let symbol_state = self.symbol_state.entry(pair.symbol.clone()).or_default();
        let alpha = parsed.factors.volatility_ewma_alpha;

        if let Some(prev_mid) = symbol_state.last_mid {
            if prev_mid > Decimal::ZERO && mid > Decimal::ZERO {
                let log_return = (mid / prev_mid).ln();
                let mean_prev = symbol_state.ewma_mean.unwrap_or(log_return);
                let mean = alpha * log_return + (Decimal::ONE - alpha) * mean_prev;
                let diff = log_return - mean_prev;
                let variance =
                    alpha * diff * diff + (Decimal::ONE - alpha) * symbol_state.ewma_variance;
                symbol_state.ewma_mean = Some(mean);
                symbol_state.ewma_variance = variance.max(Decimal::ZERO);
                symbol_state.vol_history.push_back(log_return.abs());
                while symbol_state.vol_history.len() > 512 {
                    symbol_state.vol_history.pop_front();
                }
                maybe_shrink_vecdeque(&mut symbol_state.vol_history, "factor_vol_history");
            }
        }

        symbol_state.last_mid = Some(mid);
        symbol_state.last_mid_timestamp = Some(now);

        let volatility = symbol_state.ewma_variance.sqrt().unwrap_or(Decimal::ZERO);
        let volume_window = Duration::seconds(config.factors.volume_window_secs);
        let volume_window_start = now - volume_window;
        let n_trade_window = Duration::seconds(config.factors.n_trade_window_secs);
        let n_trade_window_start = now - n_trade_window;

        let recent_trades: Vec<&RecentTrade> = market
            .recent_trades
            .iter()
            .filter(|trade| trade.timestamp >= volume_window_start)
            .collect();
        let rolling_volume: Decimal = recent_trades.iter().map(|trade| trade.quantity).sum();
        let buy_volume: Decimal = recent_trades
            .iter()
            .filter(|trade| matches!(trade.taker_side, Some(crate::domain::Side::Bid)))
            .map(|trade| trade.quantity)
            .sum();
        let sell_volume: Decimal = recent_trades
            .iter()
            .filter(|trade| matches!(trade.taker_side, Some(crate::domain::Side::Ask)))
            .map(|trade| trade.quantity)
            .sum();
        let recent_trade_count = market
            .recent_trades
            .iter()
            .filter(|trade| trade.timestamp >= n_trade_window_start)
            .count();

        // VPIN: only feed newly-seen trades into the bucket tracker so we don't
        // double-count the entire rolling window on every compute cycle.
        let mut new_buy_volume = Decimal::ZERO;
        let mut new_sell_volume = Decimal::ZERO;
        let mut new_last_trade_time = symbol_state.last_vpin_trade_time;
        let mut new_last_trade_count_at_time = symbol_state.last_vpin_trade_count_at_time;
        let mut seen_at_last_time = 0usize;
        for trade in &market.recent_trades {
            let already_processed = match symbol_state.last_vpin_trade_time {
                Some(last_time) if trade.timestamp < last_time => true,
                Some(last_time) if trade.timestamp == last_time => {
                    seen_at_last_time += 1;
                    seen_at_last_time <= symbol_state.last_vpin_trade_count_at_time
                }
                _ => false,
            };
            if already_processed {
                continue;
            }

            match trade.taker_side {
                Some(crate::domain::Side::Bid) => new_buy_volume += trade.quantity,
                Some(crate::domain::Side::Ask) => new_sell_volume += trade.quantity,
                None => {}
            }

            if Some(trade.timestamp) == new_last_trade_time {
                new_last_trade_count_at_time += 1;
            } else {
                new_last_trade_time = Some(trade.timestamp);
                new_last_trade_count_at_time = 1;
            }
        }

        if parsed.factors.vpin_bucket_size > Decimal::ZERO {
            let vpin_tracker = self
                .vpin_trackers
                .entry(pair.symbol.clone())
                .or_insert_with(|| {
                    VpinTracker::new(
                        parsed.factors.vpin_bucket_size,
                        parsed.factors.vpin_n_buckets,
                    )
                });
            vpin_tracker.add_trade(new_buy_volume, new_sell_volume);
            symbol_state.last_vpin_trade_time = new_last_trade_time;
            symbol_state.last_vpin_trade_count_at_time = new_last_trade_count_at_time;
        }
        let vpin = self
            .vpin_trackers
            .get(&pair.symbol)
            .map(|t| t.vpin())
            .unwrap_or(Decimal::ZERO);

        // S6: raw flow direction, signed [-1, 1], independent of volume magnitude.
        let flow_total = buy_volume + sell_volume;
        let raw_flow_direction = if flow_total.is_zero() {
            Decimal::ZERO
        } else {
            (buy_volume - sell_volume) / flow_total
        };
        // Issue 7: track consecutive cycles with strong raw directional pressure.
        // Used as regime fallback when EWMA hasn't caught up yet.
        if raw_flow_direction.abs() > Decimal::new(7, 1) {
            symbol_state.consecutive_high_raw_flow += 1;
        } else {
            symbol_state.consecutive_high_raw_flow = 0;
        }
        let flow_alpha = parsed
            .factors
            .flow_imbalance_weight
            .max(Decimal::new(1, 2))
            .min(Decimal::ONE);
        symbol_state.smoothed_flow = flow_alpha * raw_flow_direction
            + (Decimal::ONE - flow_alpha) * symbol_state.smoothed_flow;

        if let Some(spread_bps) = market.bbo_spread_bps() {
            symbol_state.bbo_spread_ewma = Some(match symbol_state.bbo_spread_ewma {
                Some(previous) => Decimal::new(2, 1) * spread_bps + Decimal::new(8, 1) * previous,
                None => spread_bps,
            });
        }

        let velocity_window_secs = config.factors.n_trade_window_secs.max(1);
        let trade_velocity =
            Decimal::from(recent_trade_count as u64) / Decimal::from(velocity_window_secs as u64);
        let velocity_alpha = parsed
            .factors
            .trade_velocity_alpha
            .max(Decimal::new(1, 2))
            .min(Decimal::ONE);
        symbol_state.trade_velocity_ewma = if symbol_state.trade_velocity_ewma.is_zero() {
            trade_velocity
        } else {
            velocity_alpha * trade_velocity
                + (Decimal::ONE - velocity_alpha) * symbol_state.trade_velocity_ewma
        };
        let trade_velocity_ratio = if symbol_state.trade_velocity_ewma.is_zero() {
            Decimal::ONE
        } else {
            trade_velocity / symbol_state.trade_velocity_ewma
        };

        let cross_flow = cross_symbol_flow(
            &pair.symbol,
            state,
            volume_window_start,
            parsed.factors.cross_symbol_vol_weight,
        );
        let bbo_spread_ewma_bps = symbol_state
            .bbo_spread_ewma
            .unwrap_or_else(|| market.bbo_spread_bps().unwrap_or(Decimal::ZERO));
        // B3: cap BBO spread before weighting to prevent momentary wide spreads
        // from inflating the volatility addon.
        let bbo_spread_component = bbo_spread_ewma_bps.min(parsed.factors.bbo_spread_cap_bps)
            * parsed.factors.bbo_spread_vol_weight;

        // S3: Vol-independent, convex inventory skew.
        // Formula: pos_ratio * (1 + convexity * |pos_ratio|) * effective_base
        // where effective_base = vol_floor * inventory_risk_constant (preserves existing scale).
        let Some(pair_parsed) = parsed.pair(&pair.symbol) else {
            info!(
                symbol = %pair.symbol,
                "compute() returned None: parsed pair config missing"
            );
            return None;
        };
        let current_position = state
            .positions
            .get(&pair.symbol)
            .map(|position| position.quantity)
            .unwrap_or(Decimal::ZERO);
        let pos_ratio = if pair_parsed.max_position_base.is_zero() {
            Decimal::ZERO
        } else {
            current_position / pair_parsed.max_position_base
        };
        let abs_ratio = pos_ratio.abs();
        let convexity = parsed.factors.inventory_skew_convexity;
        let effective_skew_base =
            parsed.factors.volatility_floor * parsed.factors.inventory_risk_constant;
        let inventory_skew =
            pos_ratio * (Decimal::ONE + convexity * abs_ratio) * effective_skew_base;

        // S5: Continuous regime intensity + minimum dwell time.
        let flow_abs = (symbol_state.smoothed_flow + cross_flow).abs();
        // Issue 7: raw flow fallback — 2 consecutive cycles of |raw_flow| > 0.7
        // forces TrendingToxic even if the EWMA-smoothed flow hasn't caught up.
        let force_toxic = symbol_state.consecutive_high_raw_flow >= 2;
        let target_regime = if force_toxic {
            MarketRegime::TrendingToxic
        } else {
            candidate_regime(
                flow_abs,
                volatility,
                trade_velocity_ratio,
                trade_velocity,
                recent_trade_count,
            )
        };
        let min_dwell = Duration::seconds(parsed.factors.regime_min_dwell_secs as i64);
        let can_change = symbol_state
            .regime_entered_at
            .map(|t| now - t >= min_dwell)
            .unwrap_or(true);
        if can_change && target_regime != symbol_state.regime {
            symbol_state.regime = target_regime;
            symbol_state.regime_entered_at = Some(now);
        } else if symbol_state.regime_entered_at.is_none() {
            symbol_state.regime_entered_at = Some(now);
        }
        let regime = symbol_state.regime;
        let is_toxic = regime == MarketRegime::TrendingToxic;
        symbol_state.toxic_history.push_back((now, is_toxic));
        let toxic_window_start = now - Duration::seconds(600);
        while symbol_state
            .toxic_history
            .front()
            .map(|(timestamp, _)| *timestamp < toxic_window_start)
            .unwrap_or(false)
        {
            symbol_state.toxic_history.pop_front();
        }
        maybe_shrink_vecdeque(&mut symbol_state.toxic_history, "factor_toxic_history");
        let toxic_regime_persistence_secs = symbol_state
            .toxic_history
            .iter()
            .filter(|(_, toxic)| *toxic)
            .count() as u64;

        // Continuous intensity: flow magnitude drives toxic intensity, vol drives volatile intensity.
        let flow_intensity = flow_abs.min(Decimal::ONE);
        let vol_intensity = (volatility / Decimal::new(2, 3)).min(Decimal::ONE);
        let raw_intensity = flow_intensity.max(vol_intensity);
        // EWMA-smooth the intensity to prevent jitter.
        let regime_alpha = parsed.factors.regime_intensity_alpha;
        symbol_state.regime_intensity = if symbol_state.regime_intensity.is_zero() {
            raw_intensity
        } else {
            regime_alpha * raw_intensity
                + (Decimal::ONE - regime_alpha) * symbol_state.regime_intensity
        };
        let regime_intensity = symbol_state
            .regime_intensity
            .min(Decimal::ONE)
            .max(Decimal::ZERO);

        let time_multiplier = time_of_day_multiplier(now);
        // Microprice (effective BBO size-weighted mid, already blended with raw_mid).
        let microprice = mid;
        let price_index = match pair.price_source {
            PriceSource::Mark => {
                if let Some(mark_price) = market.mark_price {
                    mark_price
                } else {
                    info!(
                        symbol = %pair.symbol,
                        microprice = %microprice,
                        "compute() missing mark_price; using microprice fallback"
                    );
                    microprice
                }
            }
            PriceSource::Spot => {
                if let Some(spot) = market.spot_price {
                    let lean_weight = parsed.factors.cex_reference_lean_weight;
                    if lean_weight > Decimal::ZERO && raw_mid > Decimal::ZERO {
                        // Add DEX book-pressure lean to CEX anchor.
                        // lean = (microprice - raw_mid) * weight
                        let lean = (microprice - raw_mid) * lean_weight;
                        spot + lean
                    } else {
                        spot
                    }
                } else {
                    microprice
                }
            }
            PriceSource::Mid => microprice,
        };
        let base_volatility_addon = factor_spread_addon(parsed, volatility);
        let volatility_addon =
            (base_volatility_addon + bbo_spread_component + cross_flow.abs()) * time_multiplier;
        let spread_floor_decay = Decimal::new(995, 3);
        if volatility_addon > symbol_state.spread_floor {
            symbol_state.spread_floor = volatility_addon;
        } else {
            symbol_state.spread_floor =
                (symbol_state.spread_floor * spread_floor_decay).max(Decimal::ZERO);
        }
        let effective_volatility_addon = volatility_addon.max(symbol_state.spread_floor);

        if symbol_state.bbo_spread_ewma.is_none() {
            info!(
                symbol = %pair.symbol,
                best_bid = ?market.best_bid,
                best_ask = ?market.best_ask,
                "compute() returned None: bbo_spread_ewma not initialized yet"
            );
            return None;
        }
        let inventory_lean_bps = (bbo_spread_ewma_bps / Decimal::TWO) * Decimal::new(7, 1);

        // S6: volume_imbalance is purely volume-based size scaling (always >= 0).
        // flow_direction carries the signed directional pressure separately.
        //
        // Bug fix: linear scaling (rolling_volume * weight) blew up to 40-50× on
        // active markets (100+ HYPE/30 s → volume_factor = 46×).  Use sqrt to
        // compress large volumes, then hard-cap at 1.5 so volume_factor ≤ 2.5×.
        let combined_flow_direction = symbol_state.smoothed_flow + cross_flow;
        let volume_size_addon = rolling_volume * parsed.factors.volume_size_weight;
        let volume_imbalance = if volume_size_addon > Decimal::ZERO {
            // sqrt(addon) compresses large values naturally; cap at 1.5 → max volume_factor = 2.5×
            volume_size_addon
                .sqrt()
                .unwrap_or(Decimal::ZERO)
                .min(Decimal::new(15, 1))
        } else {
            Decimal::ZERO
        };

        // Orderbook imbalance: leading signal from top-N book levels.
        // ob_imbalance ∈ [-1, 1]: positive = bid-heavy, negative = ask-heavy.
        let ob_imbalance = {
            let depth = parsed.factors.ob_imbalance_depth;
            let bid_depth: Decimal = if depth == 0 {
                market.bids.values().sum()
            } else {
                market.bids.iter().rev().take(depth).map(|(_, q)| *q).sum()
            };
            let ask_depth: Decimal = if depth == 0 {
                market.asks.values().sum()
            } else {
                market.asks.iter().take(depth).map(|(_, q)| *q).sum()
            };
            let total = bid_depth + ask_depth;
            if total.is_zero() {
                Decimal::ZERO
            } else {
                ((bid_depth - ask_depth) / total).clamp(-Decimal::ONE, Decimal::ONE)
            }
        };

        // Track how many consecutive cycles the flow direction has been spiking above
        // the pause threshold.  We use the same threshold from config so the counter
        // is always consistent with the pause decision in the engine.
        let flow_direction_clamped = combined_flow_direction.clamp(-Decimal::ONE, Decimal::ONE);
        let spike_threshold = parsed.factors.flow_spike_pause_threshold;
        if spike_threshold > Decimal::ZERO && flow_direction_clamped.abs() > spike_threshold {
            symbol_state.consecutive_flow_spike += 1;
        } else {
            symbol_state.consecutive_flow_spike = 0;
        }
        let consecutive_flow_spike = symbol_state.consecutive_flow_spike;

        // Fill-rate asymmetry: bid/ask fill rate over rolling window.
        let window_secs = parsed.model.fill_rate_window_secs.to_i64().unwrap_or(300);
        let fill_rate_skew = fill_tracker.fill_rate_skew(
            &pair.symbol,
            window_secs,
            parsed.model.fill_rate_skew_threshold,
        );

        // Online kappa: empirical fill probability at level 0.
        let level0_half_spread_frac = parsed.model.born_inf_bps / Decimal::from(20_000u64);
        let kappa_estimate = if parsed.model.online_kappa {
            fill_tracker.kappa_estimate(
                &pair.symbol,
                parsed.model.kappa_min_cycles,
                level0_half_spread_frac,
            )
        } else {
            None
        };

        // Funding-rate lean: positive = lean ask (receive long-funding).
        let funding_lean = if let Some(fr) = market.funding_rate {
            // fr is the periodic rate (e.g. 8-hour); convert to bps and scale.
            fr * Decimal::from(10_000u64) * parsed.model.funding_lean_weight
        } else {
            Decimal::ZERO
        };

        Some(FactorSnapshot {
            price_index,
            raw_volatility: volatility,
            volatility: effective_volatility_addon,
            inventory_lean_bps,
            volume_imbalance,
            flow_direction: flow_direction_clamped,
            inventory_skew,
            recent_trade_count,
            regime,
            regime_intensity,
            ob_imbalance,
            consecutive_flow_spike,
            microprice,
            fill_rate_skew,
            vpin,
            funding_lean,
            kappa_estimate,
            post_fill_widen_bid: Decimal::ONE, // populated by engine after compute()
            post_fill_widen_ask: Decimal::ONE,
            flow_spike_widen_multiplier: Decimal::ONE,
            toxic_regime_persistence_secs,
        })
    }
}

fn maybe_shrink_vecdeque<T>(deque: &mut VecDeque<T>, label: &str) {
    let len = deque.len();
    let capacity = deque.capacity();
    if capacity > len.saturating_mul(2).max(1024) {
        deque.shrink_to_fit();
    }
}

pub fn factor_spread_addon(parsed: &ParsedConfig, volatility: Decimal) -> Decimal {
    volatility * parsed.factors.volatility_spread_weight
}

fn cross_symbol_flow(
    symbol: &str,
    state: &BotState,
    window_start: DateTime<Utc>,
    weight: Decimal,
) -> Decimal {
    let mut total = Decimal::ZERO;
    let mut count = 0u64;
    for (other_symbol, market) in &state.market.symbols {
        if other_symbol == symbol {
            continue;
        }
        let mut buy = Decimal::ZERO;
        let mut sell = Decimal::ZERO;
        for trade in market
            .recent_trades
            .iter()
            .filter(|trade| trade.timestamp >= window_start)
        {
            match trade.taker_side {
                Some(crate::domain::Side::Bid) => buy += trade.quantity,
                Some(crate::domain::Side::Ask) => sell += trade.quantity,
                None => {}
            }
        }
        let total_volume = buy + sell;
        if total_volume > Decimal::ZERO {
            total += (buy - sell) / total_volume;
            count += 1;
        }
    }

    if count == 0 {
        Decimal::ZERO
    } else {
        (total / Decimal::from(count)) * weight
    }
}

/// S5: Pure regime candidate from signals, without dwell-time hysteresis.
fn candidate_regime(
    flow_abs: Decimal,
    volatility: Decimal,
    trade_velocity_ratio: Decimal,
    trade_velocity: Decimal,
    recent_trade_count: usize,
) -> MarketRegime {
    // B1: trade confidence gate — scale flow by how many trades back the signal.
    // With < 10 trades in the window the flow estimate is noisy; require proportionally
    // higher raw imbalance before declaring TrendingToxic.
    let confidence = Decimal::from(recent_trade_count.min(10) as u64) / Decimal::from(10u64);
    let confident_flow = flow_abs * confidence;

    // Issue 7: lower threshold from 0.6 → 0.35 so the regime fires earlier.
    if confident_flow > Decimal::new(35, 2) {
        return MarketRegime::TrendingToxic;
    }
    // Volatile: high vol, high velocity ratio vs baseline, OR raw velocity > 1.5 trades/sec
    // (= 90 trades/min). The raw velocity check catches balanced-flow high-volume activity
    // that flow_abs misses when buy and sell are equal.
    if volatility > Decimal::new(8, 4)
        || trade_velocity_ratio > Decimal::new(15, 1)
        || trade_velocity > Decimal::new(15, 1)
    {
        return MarketRegime::VolatileBalanced;
    }
    MarketRegime::Quiet
}

/// S8: GRVT/Asia-aware time-of-day spread multiplier.
/// Most liquid 0-8 UTC (Tokyo + early London) → tighter spreads.
/// Least liquid 12-20 UTC (US morning but Asian night) → wider spreads.
fn time_of_day_multiplier(now: DateTime<Utc>) -> Decimal {
    let hour = now.hour();

    let base = if (0..8).contains(&hour) {
        // Asian session: highest GRVT liquidity → narrow spreads.
        Decimal::new(8, 1)
    } else if (8..12).contains(&hour) {
        // London open / Asia close transition.
        Decimal::ONE
    } else if (12..20).contains(&hour) {
        // US morning + Asian night: least liquid for GRVT.
        Decimal::new(13, 1)
    } else {
        // Late US / early Asia pre-market.
        Decimal::new(11, 1)
    };

    // Widen during funding rate windows (every 8 h, at 0, 8, 16 UTC).
    let funding_hour = matches!(hour, 0 | 8 | 16) && now.minute() >= 45;
    let funding_next = matches!(hour, 7 | 15 | 23) && now.minute() >= 45;
    if funding_hour || funding_next {
        base * Decimal::new(15, 1)
    } else {
        base
    }
}
