use crate::{
    config::{AppConfig, ParsedConfig},
    core::state::BotState,
};
use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;

pub struct CircuitBreaker {
    open: bool,
    reason: Option<String>,
    base_cooldown: Duration,
    max_cooldown: Duration,
    backoff_decay: Duration,
    current_cooldown: Duration,
    last_tripped_at: Option<DateTime<Utc>>,
}

impl CircuitBreaker {
    pub fn new(base_cooldown_ms: u64, max_cooldown_ms: u64, backoff_decay_ms: u64) -> Self {
        let base = Duration::milliseconds(base_cooldown_ms as i64);
        Self {
            open: false,
            reason: None,
            base_cooldown: base,
            max_cooldown: Duration::milliseconds(max_cooldown_ms as i64),
            backoff_decay: Duration::milliseconds(backoff_decay_ms as i64),
            current_cooldown: base,
            last_tripped_at: None,
        }
    }

    pub fn trip(&mut self, reason: impl Into<String>) {
        let now = Utc::now();
        let reason = reason.into();
        if let Some(last_tripped_at) = self.last_tripped_at {
            if now - last_tripped_at <= self.current_cooldown {
                self.current_cooldown =
                    (self.current_cooldown + self.current_cooldown).min(self.max_cooldown);
            }
        }
        self.open = true;
        self.reason = Some(reason.clone());
        self.last_tripped_at = Some(now);
    }

    pub fn reset(&mut self) {
        let Some(last_tripped_at) = self.last_tripped_at else {
            self.open = false;
            self.reason = None;
            self.current_cooldown = self.base_cooldown;
            return;
        };
        let now = Utc::now();
        let elapsed = now - last_tripped_at;
        if elapsed < self.current_cooldown {
            return;
        }

        self.open = false;
        self.reason = None;

        if elapsed >= self.backoff_decay {
            self.current_cooldown = self.base_cooldown;
        } else {
            let reduced = self.current_cooldown - self.base_cooldown;
            self.current_cooldown = (self.base_cooldown + reduced / 2).max(self.base_cooldown);
        }
    }

    pub fn force_reset(&mut self) {
        self.open = false;
        self.reason = None;
        self.current_cooldown = self.base_cooldown;
        self.last_tripped_at = None;
    }

    pub fn ensure_closed(&self) -> Result<()> {
        if self.open {
            bail!(
                "circuit breaker open{}",
                self.reason
                    .as_ref()
                    .map(|value| format!(": {value}"))
                    .unwrap_or_default()
            );
        }
        Ok(())
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(30_000, 10 * 60_000, 5 * 60_000)
    }
}

pub fn run_security_checks(
    config: &AppConfig,
    parsed: &ParsedConfig,
    state: &BotState,
    effective_open_orders_count: usize,
) -> Result<()> {
    if state.effective_total_pnl() <= -parsed.risk.max_drawdown_usd {
        bail!("drawdown limit breached");
    }

    if state.total_abs_position_notional() > parsed.risk.max_abs_position_usd {
        bail!("global position limit breached");
    }

    for position in state.positions.values() {
        if position.quantity.is_zero() {
            continue;
        }
        let mark = state
            .market
            .symbols
            .get(&position.symbol)
            .and_then(|symbol| symbol.mark_price.or_else(|| symbol.mid_price()));
        // Price grace period: if mark is momentarily absent (e.g. WS reconnect),
        // fall back to entry_price so a brief gap doesn't trip the circuit breaker.
        let mark = match mark {
            Some(p) if p > Decimal::ZERO => p,
            _ => {
                if position.entry_price > Decimal::ZERO {
                    tracing::warn!(
                        symbol = %position.symbol,
                        entry_price = %position.entry_price,
                        "mark price unavailable; using entry price for position limit check"
                    );
                    position.entry_price
                } else {
                    bail!(
                        "no price available for position limit check on {}; cannot safely continue",
                        position.symbol
                    )
                }
            }
        };
        if (position.quantity * mark).abs() > parsed.risk.max_symbol_position_usd {
            bail!("symbol position limit breached for {}", position.symbol);
        }
    }

    if correlated_utilization(state, parsed) > Decimal::ONE {
        bail!("correlated position limit breached");
    }

    if effective_open_orders_count > config.risk.max_open_orders {
        bail!("open order count limit breached");
    }

    let stale_after_ms = config.runtime.stale_market_data_ms.max(0) as u128;
    if let Some(last_market_at) = state.market.last_updated_at {
        let age = last_market_at.elapsed().as_millis();
        if age > stale_after_ms {
            bail!("market data stale for {} ms", age);
        }
    } else {
        bail!("no market data available yet");
    }

    Ok(())
}

pub fn correlated_utilization(state: &BotState, parsed: &ParsedConfig) -> Decimal {
    if parsed.risk.max_correlated_position_usd <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    state.total_abs_position_notional() / parsed.risk.max_correlated_position_usd
}

pub fn proportional_vol_widening(volatility: Decimal, utilization: Decimal) -> Decimal {
    volatility.max(Decimal::ZERO) * utilization.max(Decimal::ZERO) * Decimal::from(10_000u64)
}
