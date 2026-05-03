use std::collections::{HashMap, HashSet, VecDeque};

use crate::domain::{MarkoutEvent, Side};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;

const OBSERVATION_WINDOW_SECS: i64 = 15;
const DECAY_TO_NEUTRAL_SECS: i64 = 20 * 60;
const MIN_SPREAD_EARNED_BPS: Decimal = Decimal::from_parts(5, 0, 0, false, 1); // 0.5
const MAX_TOXICITY_RATIO: Decimal = Decimal::from_parts(20, 0, 0, false, 0); // 20

fn neutral_score() -> Decimal {
    Decimal::new(5, 1)
}

// ─── Post-fill price tracking ────────────────────────────────────────────────

/// Tracks price at 1s / 5s / 30s after each fill for toxicity diagnostics.
struct PostFillObservation {
    symbol: String,
    side: Side,
    fill_price: Decimal,
    timestamp: DateTime<Utc>,
    price_1s: Option<Decimal>,
    price_5s: Option<Decimal>,
    price_30s: Option<Decimal>,
}

impl PostFillObservation {
    /// Feed a price observation at `now`. Returns true once all three snapshots
    /// have been taken (the entry can then be removed).
    fn observe(&mut self, price: Decimal, now: DateTime<Utc>) -> bool {
        let elapsed = (now - self.timestamp).num_milliseconds();
        if elapsed >= 1_000 && self.price_1s.is_none() {
            self.price_1s = Some(price);
        }
        if elapsed >= 5_000 && self.price_5s.is_none() {
            self.price_5s = Some(price);
        }
        if elapsed >= 30_000 && self.price_30s.is_none() {
            self.price_30s = Some(price);
            return true; // all done
        }
        false
    }

    fn into_markout_event(self, is_simulated: bool) -> Option<MarkoutEvent> {
        let Some(p30) = self.price_30s else {
            return None;
        };
        let Some(p5) = self.price_5s else {
            return None;
        };
        let Some(p1) = self.price_1s else {
            return None;
        };
        let base = self.fill_price;
        if base <= Decimal::ZERO {
            return None;
        }
        let bps = |p: Decimal| {
            let raw = match self.side {
                Side::Bid => (base - p) / base,
                Side::Ask => (p - base) / base,
            };
            raw * Decimal::from(10_000u64)
        };
        Some(MarkoutEvent {
            ts: self.timestamp + Duration::seconds(30),
            symbol: self.symbol,
            side: self.side,
            fill_price: self.fill_price,
            fill_ts: self.timestamp,
            price_1s: p1,
            price_5s: p5,
            price_30s: p30,
            markout_1s_bps: bps(p1),
            markout_5s_bps: bps(p5),
            markout_30s_bps: bps(p30),
            is_simulated,
        })
    }
}

// ─── Fill-rate tracker ────────────────────────────────────────────────────────

/// Per-symbol rolling fill-rate counts used to detect bid/ask asymmetry.
struct FillRateState {
    bid_timestamps: VecDeque<DateTime<Utc>>,
    ask_timestamps: VecDeque<DateTime<Utc>>,
}

impl FillRateState {
    fn record(&mut self, side: Side, now: DateTime<Utc>) {
        match side {
            Side::Bid => self.bid_timestamps.push_back(now),
            Side::Ask => self.ask_timestamps.push_back(now),
        }
    }

    fn purge_old(&mut self, cutoff: DateTime<Utc>) {
        while self
            .bid_timestamps
            .front()
            .map(|&t| t < cutoff)
            .unwrap_or(false)
        {
            self.bid_timestamps.pop_front();
        }
        while self
            .ask_timestamps
            .front()
            .map(|&t| t < cutoff)
            .unwrap_or(false)
        {
            self.ask_timestamps.pop_front();
        }
    }

    /// Signed [-1, 1]: positive = bid fills faster.
    fn skew(&self, window_secs: i64, threshold: Decimal, now: DateTime<Utc>) -> Decimal {
        let cutoff = now - Duration::seconds(window_secs);
        let bid = self.bid_timestamps.iter().filter(|&&t| t >= cutoff).count() as u64;
        let ask = self.ask_timestamps.iter().filter(|&&t| t >= cutoff).count() as u64;
        let bid_d = Decimal::from(bid + 1); // +1 Laplace smoothing
        let ask_d = Decimal::from(ask + 1);
        let ratio = bid_d / ask_d;
        if ratio > threshold {
            // bid fills much faster — clamp to 1
            (ratio / threshold - Decimal::ONE).min(Decimal::ONE)
        } else if ask_d / bid_d > threshold {
            -((ask_d / bid_d) / threshold - Decimal::ONE).min(Decimal::ONE)
        } else {
            Decimal::ZERO
        }
    }
}

impl Default for FillRateState {
    fn default() -> Self {
        Self {
            bid_timestamps: VecDeque::new(),
            ask_timestamps: VecDeque::new(),
        }
    }
}

// ─── Online kappa tracker ─────────────────────────────────────────────────────

/// Per-symbol EWMA-based fill-probability estimator at level 0.
/// Derives an online κ from `P(fill) ≈ exp(-κ · δ/2)`.
struct KappaState {
    /// EWMA of was-filled (1.0) vs not-filled (0.0) at level 0.
    fill_prob_ewma: Option<Decimal>,
    cycles: u64,
}

impl KappaState {
    fn record_cycle(&mut self, was_filled: bool) {
        let alpha = Decimal::new(5, 2); // 0.05
        let sample = if was_filled {
            Decimal::ONE
        } else {
            Decimal::ZERO
        };
        self.fill_prob_ewma = Some(match self.fill_prob_ewma {
            None => sample,
            Some(prev) => alpha * sample + (Decimal::ONE - alpha) * prev,
        });
        self.cycles += 1;
    }

    /// Returns estimated κ given the level-0 half-spread fraction.
    fn kappa_estimate(&self, min_cycles: u64, half_spread_frac: Decimal) -> Option<Decimal> {
        use rust_decimal::MathematicalOps;
        if self.cycles < min_cycles {
            return None;
        }
        let prob = self.fill_prob_ewma?;
        let prob_clamped = prob.clamp(Decimal::new(1, 4), Decimal::new(9999, 4)); // [0.0001, 0.9999]
        if half_spread_frac <= Decimal::ZERO {
            return None;
        }
        let neg_ln = -(prob_clamped.ln());
        Some((neg_ln / half_spread_frac).clamp(Decimal::new(1, 6), Decimal::from(8_000u64)))
    }
}

impl Default for KappaState {
    fn default() -> Self {
        Self {
            fill_prob_ewma: None,
            cycles: 0,
        }
    }
}

// ─── Main FillTracker ────────────────────────────────────────────────────────

#[derive(Default)]
pub struct FillTracker {
    pending: VecDeque<TrackedFill>,
    scores: HashMap<String, HashMap<(Side, usize), LevelToxicityScore>>,
    post_fill: HashMap<String, Vec<PostFillObservation>>,
    markout_30s_history: HashMap<(String, Side), VecDeque<Decimal>>,
    fill_rate: HashMap<String, FillRateState>,
    kappa: HashMap<String, KappaState>,
    /// Symbols that had a level-0 fill since the last `record_quote_cycle` call.
    pending_l0_fills: HashSet<String>,
}

impl FillTracker {
    pub fn record_fill(
        &mut self,
        symbol: String,
        side: Side,
        level_index: usize,
        fill_price: Decimal,
        spread_earned_bps: Decimal,
        timestamp: DateTime<Utc>,
    ) {
        // Record for toxicity scoring.
        self.pending.push_back(TrackedFill {
            symbol: symbol.clone(),
            side,
            level_index,
            fill_price,
            spread_earned_bps: spread_earned_bps.max(MIN_SPREAD_EARNED_BPS),
            timestamp,
        });
        // Record level-0 fill for online kappa estimation.
        if level_index == 0 {
            self.pending_l0_fills.insert(symbol.clone());
        }
        // Record for post-fill price tracking.
        self.post_fill
            .entry(symbol.clone())
            .or_default()
            .push(PostFillObservation {
                symbol: symbol.clone(),
                side,
                fill_price,
                timestamp,
                price_1s: None,
                price_5s: None,
                price_30s: None,
            });
        // Record for fill-rate asymmetry.
        self.fill_rate
            .entry(symbol)
            .or_default()
            .record(side, timestamp);
    }

    /// Call every reconcile cycle for each quoted symbol.
    /// Consumes any pending level-0 fill recorded by `record_fill`.
    pub fn record_quote_cycle(&mut self, symbol: &str) {
        let was_filled = self.pending_l0_fills.remove(symbol);
        self.kappa
            .entry(symbol.to_string())
            .or_default()
            .record_cycle(was_filled);
    }

    pub fn observe_price(
        &mut self,
        symbol: &str,
        reference_price: Decimal,
        timestamp: DateTime<Utc>,
        is_simulated: bool,
    ) -> Vec<MarkoutEvent> {
        if reference_price <= Decimal::ZERO {
            return Vec::new();
        }

        let mut completed_markouts = Vec::new();

        // Toxicity observation (15-second window).
        let mut finished = Vec::new();
        let mut remaining = VecDeque::with_capacity(self.pending.len());
        while let Some(tracked) = self.pending.pop_front() {
            if tracked.symbol != symbol {
                remaining.push_back(tracked);
                continue;
            }
            if timestamp - tracked.timestamp >= Duration::seconds(OBSERVATION_WINDOW_SECS) {
                finished.push((tracked, reference_price, timestamp));
            } else {
                remaining.push_back(tracked);
            }
        }
        self.pending = remaining;

        for (tracked, observed_price, observed_at) in finished {
            let adverse_bps = adverse_bps(tracked.side, tracked.fill_price, observed_price);
            let toxicity_ratio = (adverse_bps
                / tracked.spread_earned_bps.max(MIN_SPREAD_EARNED_BPS))
            .min(MAX_TOXICITY_RATIO);
            let score = self
                .scores
                .entry(tracked.symbol)
                .or_default()
                .entry((tracked.side, tracked.level_index))
                .or_default();
            score.apply(toxicity_ratio, observed_at);
        }

        // Post-fill price observations (1s / 5s / 30s).
        if let Some(observations) = self.post_fill.get_mut(symbol) {
            let mut keep = Vec::new();
            for mut obs in observations.drain(..) {
                let done = obs.observe(reference_price, timestamp);
                if done {
                    if let Some(event) = obs.into_markout_event(is_simulated) {
                        self.markout_30s_history
                            .entry((event.symbol.clone(), event.side))
                            .or_default()
                            .push_back(event.markout_30s_bps);
                        if let Some(history) = self
                            .markout_30s_history
                            .get_mut(&(event.symbol.clone(), event.side))
                        {
                            while history.len() > 200 {
                                history.pop_front();
                            }
                        }
                        completed_markouts.push(event);
                    }
                } else {
                    keep.push(obs);
                }
            }
            *observations = keep;
        }

        completed_markouts
    }

    pub fn decay(&mut self, now: DateTime<Utc>) {
        for levels in self.scores.values_mut() {
            for score in levels.values_mut() {
                score.decay(now);
            }
        }
        // Purge old fill-rate timestamps (keep last hour).
        let cutoff = now - Duration::hours(1);
        for state in self.fill_rate.values_mut() {
            state.purge_old(cutoff);
        }
    }

    pub fn toxicity_score(&self, symbol: &str, side: Side, level_index: usize) -> Decimal {
        self.scores
            .get(symbol)
            .and_then(|levels| levels.get(&(side, level_index)))
            .map(|score| score.value)
            .unwrap_or_else(neutral_score)
    }

    pub fn level_volume_multiplier(&self, symbol: &str, side: Side, level_index: usize) -> Decimal {
        let score = self.toxicity_score(symbol, side, level_index);
        (Decimal::TWO - score).clamp(Decimal::new(5, 1), Decimal::new(15, 1))
    }

    /// Signed fill-rate skew [-1, 1]: positive = bid fills faster.
    pub fn fill_rate_skew(&self, symbol: &str, window_secs: i64, threshold: Decimal) -> Decimal {
        self.fill_rate
            .get(symbol)
            .map(|s| s.skew(window_secs, threshold, Utc::now()))
            .unwrap_or(Decimal::ZERO)
    }

    /// Online kappa estimate from empirical fill probability at level 0.
    /// Returns None until `min_cycles` have been observed.
    pub fn kappa_estimate(
        &self,
        symbol: &str,
        min_cycles: u64,
        half_spread_frac: Decimal,
    ) -> Option<Decimal> {
        self.kappa
            .get(symbol)?
            .kappa_estimate(min_cycles, half_spread_frac)
    }

    pub fn trailing_markout_30s_avg_bps(
        &self,
        symbol: &str,
        side: Side,
        window: usize,
    ) -> Option<Decimal> {
        let history = self.markout_30s_history.get(&(symbol.to_string(), side))?;
        let take = history.len().min(window);
        if take == 0 {
            return None;
        }
        let sum: Decimal = history.iter().rev().take(take).copied().sum();
        Some(sum / Decimal::from(take as u64))
    }
}

// ─── Internal structs ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct TrackedFill {
    symbol: String,
    side: Side,
    level_index: usize,
    fill_price: Decimal,
    spread_earned_bps: Decimal,
    timestamp: DateTime<Utc>,
}

#[derive(Default)]
struct LevelToxicityScore {
    value: Decimal,
    last_updated: Option<DateTime<Utc>>,
}

impl LevelToxicityScore {
    fn apply(&mut self, toxicity_ratio: Decimal, now: DateTime<Utc>) {
        self.decay(now);
        let alpha = Decimal::new(25, 2);
        if self.last_updated.is_none() || self.value.is_zero() {
            self.value = toxicity_ratio;
        } else {
            self.value = alpha * toxicity_ratio + (Decimal::ONE - alpha) * self.value;
        }
        self.last_updated = Some(now);
    }

    fn decay(&mut self, now: DateTime<Utc>) {
        let Some(last_updated) = self.last_updated else {
            if self.value.is_zero() {
                self.value = neutral_score();
            }
            return;
        };
        let elapsed = (now - last_updated).num_seconds().max(0) as f64;
        let half_life = DECAY_TO_NEUTRAL_SECS as f64;
        let decay = if half_life <= 0.0 {
            0.0
        } else {
            0.5f64.powf(elapsed / half_life)
        };
        if decay.is_nan() || decay.is_infinite() {
            self.last_updated = Some(now);
            return;
        }
        let decay = Decimal::from_f64_retain(decay).unwrap_or(Decimal::ONE);
        let neutral = neutral_score();
        self.value = neutral + (self.value - neutral) * decay;
        self.last_updated = Some(now);
    }
}

fn adverse_bps(side: Side, fill_price: Decimal, observed_price: Decimal) -> Decimal {
    if fill_price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let raw = match side {
        Side::Bid => (fill_price - observed_price) / fill_price,
        Side::Ask => (observed_price - fill_price) / fill_price,
    };
    raw.max(Decimal::ZERO) * Decimal::from(10_000u64)
}
