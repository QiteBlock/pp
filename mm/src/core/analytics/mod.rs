use rust_decimal::Decimal;

use crate::{config::AppConfig, core::state::BotState, domain::Side};

pub mod kappa;
pub mod markout;
pub mod toxicity;
pub mod tracker;

#[derive(Default)]
pub struct MinuteStats {
    pub trade_count: usize,
    pub fills_count: usize,
    pub simulated_fills_count: usize,
    pub missed_crosses_count: usize,
    pub near_misses_count: usize,
    pub position_sample_count: usize,
    pub signed_position_sum: Decimal,
    /// Accumulated spread_earned_bps across all fills this minute.
    pub spread_earned_bps_sum: Decimal,
    /// Accumulated fill notional (qty * price) for spread-earned averaging.
    pub fill_notional_sum: Decimal,
    /// Number of fills where adverse selection > spread earned (toxic fills).
    pub toxic_fill_count: usize,
}

impl MinuteStats {
    pub fn record_position_sample(&mut self, config: &AppConfig, state: &BotState) {
        let signed_position: Decimal = config
            .pairs
            .iter()
            .filter(|pair| pair.enabled)
            .map(|pair| {
                state
                    .positions
                    .get(&pair.symbol)
                    .map(|position| position.quantity)
                    .unwrap_or(Decimal::ZERO)
            })
            .sum();
        self.position_sample_count += 1;
        self.signed_position_sum += signed_position;
    }

    pub fn accumulate_fill(&mut self, fs: FillStats) {
        if !fs.is_simulated {
            self.fills_count += 1;
        } else {
            self.fills_count += 1;
            self.simulated_fills_count += 1;
        }
        self.spread_earned_bps_sum += fs.spread_earned_bps * fs.notional;
        self.fill_notional_sum += fs.notional;
        if fs.is_toxic {
            self.toxic_fill_count += 1;
        }
    }
}

#[derive(Clone)]
pub struct FillStats {
    pub spread_earned_bps: Decimal,
    pub notional: Decimal,
    pub is_toxic: bool,
    pub is_simulated: bool,
    pub toxicity_score: Decimal,
    pub fill_symbol: String,
    pub fill_side: Side,
}
