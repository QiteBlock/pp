use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StrategyMode {
    #[default]
    Mm,
    Asymmetric,
    SizeSkew,
    Sniper,
}

impl FromStr for StrategyMode {
    type Err = anyhow::Error;

    fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "mm" => Ok(Self::Mm),
            "asymmetric" => Ok(Self::Asymmetric),
            "size_skew" => Ok(Self::SizeSkew),
            "sniper" => Ok(Self::Sniper),
            other => bail!("unsupported strategy_mode: {other}"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SniperConfig {
    pub drift_threshold_bps: Decimal,
    pub window: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct SniperState {
    pub snipe_bid_until: Option<Instant>,
    pub snipe_ask_until: Option<Instant>,
    pub drift_skip_bid_until: Option<Instant>,
    pub drift_skip_ask_until: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateReason {
    EmergencyTwoSided,
    MmTwoSided,
    AsymmetricFlatTwoSided,
    AsymmetricLongAskOnly,
    AsymmetricShortBidOnly,
    SizeSkewTwoSided,
    SniperIdle,
    SniperBidWindow,
    SniperAskWindow,
    SniperBothWindows,
    SniperLongUnwindOnly,
    SniperShortUnwindOnly,
    SniperBidWindowPlusLongUnwind,
    SniperAskWindowPlusShortUnwind,
}

impl GateReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EmergencyTwoSided => "emergency_two_sided",
            Self::MmTwoSided => "mm_two_sided",
            Self::AsymmetricFlatTwoSided => "asymmetric_flat_two_sided",
            Self::AsymmetricLongAskOnly => "asymmetric_long_ask_only",
            Self::AsymmetricShortBidOnly => "asymmetric_short_bid_only",
            Self::SizeSkewTwoSided => "size_skew_two_sided",
            Self::SniperIdle => "sniper_idle",
            Self::SniperBidWindow => "sniper_bid_window",
            Self::SniperAskWindow => "sniper_ask_window",
            Self::SniperBothWindows => "sniper_both_windows",
            Self::SniperLongUnwindOnly => "sniper_long_unwind_only",
            Self::SniperShortUnwindOnly => "sniper_short_unwind_only",
            Self::SniperBidWindowPlusLongUnwind => "sniper_bid_window_plus_long_unwind",
            Self::SniperAskWindowPlusShortUnwind => "sniper_ask_window_plus_short_unwind",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QuoteGate {
    pub bid_enabled: bool,
    pub ask_enabled: bool,
    pub reason: GateReason,
}

pub fn evaluate_gate(
    mode: StrategyMode,
    position: Decimal,
    dust_threshold: Decimal,
    emergency_ratio: Decimal,
    max_position_base: Decimal,
    sniper: SniperConfig,
    drift_bps: Option<Decimal>,
    sniper_state: &mut SniperState,
    now: Instant,
) -> QuoteGate {
    if max_position_base > Decimal::ZERO && position.abs() >= max_position_base * emergency_ratio {
        return QuoteGate {
            bid_enabled: true,
            ask_enabled: true,
            reason: GateReason::EmergencyTwoSided,
        };
    }

    match mode {
        StrategyMode::Mm => QuoteGate {
            bid_enabled: true,
            ask_enabled: true,
            reason: GateReason::MmTwoSided,
        },
        StrategyMode::SizeSkew => QuoteGate {
            bid_enabled: true,
            ask_enabled: true,
            reason: GateReason::SizeSkewTwoSided,
        },
        StrategyMode::Asymmetric => {
            if position.abs() <= dust_threshold {
                QuoteGate {
                    bid_enabled: true,
                    ask_enabled: true,
                    reason: GateReason::AsymmetricFlatTwoSided,
                }
            } else if position > Decimal::ZERO {
                QuoteGate {
                    bid_enabled: false,
                    ask_enabled: true,
                    reason: GateReason::AsymmetricLongAskOnly,
                }
            } else {
                QuoteGate {
                    bid_enabled: true,
                    ask_enabled: false,
                    reason: GateReason::AsymmetricShortBidOnly,
                }
            }
        }
        StrategyMode::Sniper => {
            if let Some(drift_bps) = drift_bps {
                if drift_bps <= -sniper.drift_threshold_bps {
                    sniper_state.snipe_bid_until = Some(now + sniper.window);
                } else if drift_bps >= sniper.drift_threshold_bps {
                    sniper_state.snipe_ask_until = Some(now + sniper.window);
                }
            }

            let bid_window_active = sniper_state
                .snipe_bid_until
                .map(|until| until > now)
                .unwrap_or(false);
            let ask_window_active = sniper_state
                .snipe_ask_until
                .map(|until| until > now)
                .unwrap_or(false);
            let unwind_bid = position < -dust_threshold;
            let unwind_ask = position > dust_threshold;
            let bid_enabled = bid_window_active || unwind_bid;
            let ask_enabled = ask_window_active || unwind_ask;
            let reason = if bid_window_active && unwind_ask {
                GateReason::SniperBidWindowPlusLongUnwind
            } else if ask_window_active && unwind_bid {
                GateReason::SniperAskWindowPlusShortUnwind
            } else if unwind_ask {
                GateReason::SniperLongUnwindOnly
            } else if unwind_bid {
                GateReason::SniperShortUnwindOnly
            } else if bid_window_active && ask_window_active {
                GateReason::SniperBothWindows
            } else if bid_window_active {
                GateReason::SniperBidWindow
            } else if ask_window_active {
                GateReason::SniperAskWindow
            } else {
                GateReason::SniperIdle
            };

            QuoteGate {
                bid_enabled,
                ask_enabled,
                reason,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mm_mode_keeps_both_sides_enabled() {
        let mut sniper_state = SniperState::default();
        let gate = evaluate_gate(
            StrategyMode::Mm,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::new(85, 2),
            Decimal::ONE,
            SniperConfig {
                drift_threshold_bps: Decimal::from(10u64),
                window: Duration::from_secs(5),
            },
            None,
            &mut sniper_state,
            Instant::now(),
        );
        assert!(gate.bid_enabled);
        assert!(gate.ask_enabled);
        assert_eq!(gate.reason, GateReason::MmTwoSided);
    }

    #[test]
    fn asymmetric_mode_unwinds_only_the_inventory_side() {
        let now = Instant::now();
        let mut sniper_state = SniperState::default();
        let long_gate = evaluate_gate(
            StrategyMode::Asymmetric,
            Decimal::ONE,
            Decimal::new(1, 3),
            Decimal::new(85, 2),
            Decimal::from(10u64),
            SniperConfig {
                drift_threshold_bps: Decimal::from(10u64),
                window: Duration::from_secs(5),
            },
            None,
            &mut sniper_state,
            now,
        );
        assert!(!long_gate.bid_enabled);
        assert!(long_gate.ask_enabled);
        assert_eq!(long_gate.reason, GateReason::AsymmetricLongAskOnly);

        let short_gate = evaluate_gate(
            StrategyMode::Asymmetric,
            -Decimal::ONE,
            Decimal::new(1, 3),
            Decimal::new(85, 2),
            Decimal::from(10u64),
            SniperConfig {
                drift_threshold_bps: Decimal::from(10u64),
                window: Duration::from_secs(5),
            },
            None,
            &mut sniper_state,
            now,
        );
        assert!(short_gate.bid_enabled);
        assert!(!short_gate.ask_enabled);
        assert_eq!(short_gate.reason, GateReason::AsymmetricShortBidOnly);
    }

    #[test]
    fn size_skew_mode_keeps_both_sides_enabled() {
        let now = Instant::now();
        let mut sniper_state = SniperState::default();
        let gate = evaluate_gate(
            StrategyMode::SizeSkew,
            Decimal::ONE,
            Decimal::new(1, 3),
            Decimal::new(85, 2),
            Decimal::from(10u64),
            SniperConfig {
                drift_threshold_bps: Decimal::from(10u64),
                window: Duration::from_secs(5),
            },
            None,
            &mut sniper_state,
            now,
        );
        assert!(gate.bid_enabled);
        assert!(gate.ask_enabled);
        assert_eq!(gate.reason, GateReason::SizeSkewTwoSided);
    }

    #[test]
    fn sniper_mode_opens_bid_window_on_negative_drift() {
        let now = Instant::now();
        let mut sniper_state = SniperState::default();
        let gate = evaluate_gate(
            StrategyMode::Sniper,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::new(85, 2),
            Decimal::ONE,
            SniperConfig {
                drift_threshold_bps: Decimal::from(10u64),
                window: Duration::from_secs(5),
            },
            Some(-Decimal::from(12u64)),
            &mut sniper_state,
            now,
        );
        assert!(gate.bid_enabled);
        assert!(!gate.ask_enabled);
        assert_eq!(gate.reason, GateReason::SniperBidWindow);
    }

    #[test]
    fn sniper_mode_keeps_unwind_side_enabled_while_holding_inventory() {
        let now = Instant::now();
        let mut sniper_state = SniperState::default();
        let gate = evaluate_gate(
            StrategyMode::Sniper,
            Decimal::ONE,
            Decimal::new(1, 3),
            Decimal::new(85, 2),
            Decimal::from(10u64),
            SniperConfig {
                drift_threshold_bps: Decimal::from(10u64),
                window: Duration::from_secs(5),
            },
            None,
            &mut sniper_state,
            now,
        );
        assert!(!gate.bid_enabled);
        assert!(gate.ask_enabled);
        assert_eq!(gate.reason, GateReason::SniperLongUnwindOnly);
    }

    #[test]
    fn emergency_override_enables_both_sides_regardless_of_mode() {
        let now = Instant::now();
        let mut sniper_state = SniperState::default();
        let gate = evaluate_gate(
            StrategyMode::Asymmetric,
            Decimal::new(9, 1),
            Decimal::new(1, 3),
            Decimal::new(85, 2),
            Decimal::ONE,
            SniperConfig {
                drift_threshold_bps: Decimal::from(10u64),
                window: Duration::from_secs(5),
            },
            None,
            &mut sniper_state,
            now,
        );
        assert!(gate.bid_enabled);
        assert!(gate.ask_enabled);
        assert_eq!(gate.reason, GateReason::EmergencyTwoSided);
    }
}
