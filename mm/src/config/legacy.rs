use std::{collections::HashMap, fs, path::Path};

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AppConfig {
    pub venue: VenueConfig,
    pub runtime: RuntimeConfig,
    pub network: NetworkConfig,
    pub model: ModelConfig,
    pub risk: RiskConfig,
    pub factors: FactorConfig,
    #[serde(default)]
    pub hedging: HedgingConfig,
    pub storage: Option<StorageConfig>,
    pub telegram: Option<TelegramConfig>,
    pub pairs: Vec<PairConfig>,
}

impl AppConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())?;
        let config: Self = toml::from_str(&content)
            .with_context(|| format!("failed to parse config at {}", path.as_ref().display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let parsed = self.parsed()?;
        if parsed.model.born_inf_bps < Decimal::ZERO || parsed.model.born_sup_bps < Decimal::ZERO {
            bail!("model spreads must be non-negative");
        }
        if parsed.model.born_inf_bps >= parsed.model.born_sup_bps {
            bail!("model.born_inf_bps must be strictly less than model.born_sup_bps");
        }
        if parsed.model.max_spread_bps < parsed.model.born_sup_bps {
            bail!("model.max_spread_bps must be >= model.born_sup_bps");
        }
        if parsed.venue.taker_fee_rate < Decimal::ZERO {
            bail!("venue.max_fee_rate must be non-negative");
        }
        if parsed.hedging.hyperliquid_taker_fee_rate < Decimal::ZERO {
            bail!("hedging.hyperliquid_taker_fee_rate must be non-negative");
        }
        if parsed.runtime.reconcile_interval_ms == 0 {
            bail!("runtime.reconcile_interval_ms must be > 0");
        }
        if parsed.runtime.websocket_send_timeout_ms == 0 {
            bail!("runtime.websocket_send_timeout_ms must be > 0");
        }
        if parsed.runtime.max_orderbook_depth == 0 {
            bail!("runtime.max_orderbook_depth must be > 0");
        }
        if parsed.model.price_sensitivity_threshold < Decimal::ZERO
            || parsed.model.price_sensitivity_threshold > Decimal::ONE
        {
            bail!("model.price_sensitivity_threshold must be within [0, 1]");
        }
        if parsed.model.price_sensitivity_scaling_factor < Decimal::ZERO
            || parsed.model.price_sensitivity_scaling_factor > Decimal::ONE
        {
            bail!("model.price_sensitivity_scaling_factor must be within [0, 1]");
        }
        if parsed.risk.stale_position_close_slippage_cap_bps < Decimal::ZERO {
            bail!("risk.stale_position_close_slippage_cap_bps must be non-negative");
        }
        if parsed.risk.stale_position_close_chunk_base < Decimal::ZERO {
            bail!("risk.stale_position_close_chunk_base must be non-negative");
        }
        if parsed.factors.regime_intensity_alpha <= Decimal::ZERO
            || parsed.factors.regime_intensity_alpha > Decimal::ONE
        {
            bail!("factors.regime_intensity_alpha must be within (0, 1]");
        }
        if parsed.factors.ob_imbalance_depth > parsed.runtime.max_orderbook_depth {
            bail!(
                "factors.ob_imbalance_depth ({}) must be <= runtime.max_orderbook_depth ({})",
                parsed.factors.ob_imbalance_depth,
                parsed.runtime.max_orderbook_depth
            );
        }
        for pair in &parsed.pairs {
            if pair.max_position_base < Decimal::ZERO {
                bail!(
                    "pair {} max_position_base must be non-negative",
                    pair.symbol
                );
            }
        }
        Ok(())
    }

    pub fn parsed(&self) -> Result<ParsedConfig> {
        Ok(ParsedConfig {
            venue: ParsedVenueConfig {
                maker_fee_rate: parse_decimal(
                    "venue.maker_fee_ratio",
                    &self.venue.maker_fee_ratio,
                )?,
                taker_fee_rate: parse_decimal("venue.max_fee_rate", &self.venue.max_fee_rate)?,
            },
            runtime: ParsedRuntimeConfig {
                reconcile_interval_ms: self.runtime.reconcile_interval_ms,
                websocket_send_timeout_ms: self.runtime.websocket_send_timeout_ms,
                max_orderbook_depth: self.runtime.max_orderbook_depth,
                restore_dry_run_state: self.runtime.restore_dry_run_state,
            },
            model: ParsedModelConfig {
                born_inf_bps: parse_decimal("model.born_inf_bps", &self.model.born_inf_bps)?,
                born_sup_bps: parse_decimal("model.born_sup_bps", &self.model.born_sup_bps)?,
                overnight_born_inf_bps: parse_decimal(
                    "model.overnight_born_inf_bps",
                    &self.model.overnight_born_inf_bps,
                )?,
                overnight_start_hour_utc: self.model.overnight_start_hour_utc,
                overnight_end_hour_utc: self.model.overnight_end_hour_utc,
                v0: parse_decimal("model.v0", &self.model.v0)?,
                mu: parse_decimal("model.mu", &self.model.mu)?,
                sigma: parse_decimal("model.sigma", &self.model.sigma)?,
                min_step_price: parse_decimal("model.min_step_price", &self.model.min_step_price)?,
                min_step_volume: parse_decimal(
                    "model.min_step_volume",
                    &self.model.min_step_volume,
                )?,
                min_trade_amount: parse_decimal(
                    "model.min_trade_amount",
                    &self.model.min_trade_amount,
                )?,
                position_size_skew_weight: parse_decimal(
                    "model.position_size_skew_weight",
                    &self.model.position_size_skew_weight,
                )?,
                position_spread_multiplier: parse_decimal(
                    "model.position_spread_multiplier",
                    &self.model.position_spread_multiplier,
                )?,
                position_dead_zone: parse_decimal(
                    "model.position_dead_zone",
                    &self.model.position_dead_zone,
                )?,
                price_sensitivity_threshold: parse_decimal(
                    "model.price_sensitivity_threshold",
                    &self.model.price_sensitivity_threshold,
                )?,
                price_sensitivity_scaling_factor: parse_decimal(
                    "model.price_sensitivity_scaling_factor",
                    &self.model.price_sensitivity_scaling_factor,
                )?,
                volume_sensitivity_threshold: parse_decimal(
                    "model.volume_sensitivity_threshold",
                    &self.model.volume_sensitivity_threshold,
                )?,
                volatility_cut_threshold: parse_decimal(
                    "model.volatility_cut_threshold",
                    &self.model.volatility_cut_threshold,
                )?,
                index_forward_buffer_bps: parse_decimal(
                    "model.index_forward_buffer_bps",
                    &self.model.index_forward_buffer_bps,
                )?,
                max_spread_bps: parse_decimal("model.max_spread_bps", &self.model.max_spread_bps)?,
                as_gamma: parse_decimal("model.as_gamma", &self.model.as_gamma)?,
                as_kappa: parse_decimal("model.as_kappa", &self.model.as_kappa)?,
                as_time_horizon: parse_decimal(
                    "model.as_time_horizon",
                    &self.model.as_time_horizon,
                )?,
                inventory_lean_bps: parse_decimal(
                    "model.inventory_lean_bps",
                    &self.model.inventory_lean_bps,
                )?,
                as_vol_cap: parse_decimal("model.as_vol_cap", &self.model.as_vol_cap)?,
                fill_rate_window_secs: parse_decimal(
                    "model.fill_rate_window_secs",
                    &self.model.fill_rate_window_secs,
                )?,
                fill_rate_skew_threshold: parse_decimal(
                    "model.fill_rate_skew_threshold",
                    &self.model.fill_rate_skew_threshold,
                )?,
                fill_rate_competitive_bps: parse_decimal(
                    "model.fill_rate_competitive_bps",
                    &self.model.fill_rate_competitive_bps,
                )?,
                vpin_widen_multiplier: parse_decimal(
                    "model.vpin_widen_multiplier",
                    &self.model.vpin_widen_multiplier,
                )?,
                funding_lean_weight: parse_decimal(
                    "model.funding_lean_weight",
                    &self.model.funding_lean_weight,
                )?,
                post_fill_widen_secs: self
                    .model
                    .post_fill_widen_secs
                    .parse::<u64>()
                    .with_context(|| "invalid u64 for model.post_fill_widen_secs")?,
                post_fill_widen_multiplier: parse_decimal(
                    "model.post_fill_widen_multiplier",
                    &self.model.post_fill_widen_multiplier,
                )?,
                online_kappa: self
                    .model
                    .online_kappa
                    .parse::<bool>()
                    .with_context(|| "invalid bool for model.online_kappa")?,
                kappa_min_cycles: self
                    .model
                    .kappa_min_cycles
                    .parse::<u64>()
                    .with_context(|| "invalid u64 for model.kappa_min_cycles")?,
            },
            risk: ParsedRiskConfig {
                max_abs_position_usd: parse_decimal(
                    "risk.max_abs_position_usd",
                    &self.risk.max_abs_position_usd,
                )?,
                max_symbol_position_usd: parse_decimal(
                    "risk.max_symbol_position_usd",
                    &self.risk.max_symbol_position_usd,
                )?,
                max_drawdown_usd: parse_decimal(
                    "risk.max_drawdown_usd",
                    &self.risk.max_drawdown_usd,
                )?,
                emergency_widening_bps: parse_decimal(
                    "risk.emergency_widening_bps",
                    &self.risk.emergency_widening_bps,
                )?,
                emergency_skew_start: parse_decimal(
                    "risk.emergency_skew_start",
                    &self.risk.emergency_skew_start,
                )?,
                emergency_skew_max: parse_decimal(
                    "risk.emergency_skew_max",
                    &self.risk.emergency_skew_max,
                )?,
                max_correlated_position_usd: parse_decimal(
                    "risk.max_correlated_position_usd",
                    &self.risk.max_correlated_position_usd,
                )?,
                circuit_breaker_cooldown_ms: self.risk.circuit_breaker_cooldown_ms,
                emergency_unwind_threshold: parse_decimal(
                    "risk.emergency_unwind_threshold",
                    &self.risk.emergency_unwind_threshold,
                )?,
                emergency_unwind_cycles: self.risk.emergency_unwind_cycles,
                stale_position_timeout_secs: self.risk.stale_position_timeout_secs,
                stale_position_bid_hold_secs: self.risk.stale_position_bid_hold_secs,
                stale_position_ask_hold_secs: self.risk.stale_position_ask_hold_secs,
                stale_position_close_slippage_cap_bps: parse_decimal(
                    "risk.stale_position_close_slippage_cap_bps",
                    &self.risk.stale_position_close_slippage_cap_bps,
                )?,
                stale_position_close_chunk_base: parse_decimal(
                    "risk.stale_position_close_chunk_base",
                    &self.risk.stale_position_close_chunk_base,
                )?,
            },
            factors: ParsedFactorConfig {
                volatility_ewma_alpha: parse_decimal(
                    "factors.volatility_ewma_alpha",
                    &self.factors.volatility_ewma_alpha,
                )?,
                volatility_spread_weight: parse_decimal(
                    "factors.volatility_spread_weight",
                    &self.factors.volatility_spread_weight,
                )?,
                inventory_skew_weight: parse_decimal(
                    "factors.inventory_skew_weight",
                    &self.factors.inventory_skew_weight,
                )?,
                volume_size_weight: parse_decimal(
                    "factors.volume_size_weight",
                    &self.factors.volume_size_weight,
                )?,
                flow_imbalance_weight: parse_decimal(
                    "factors.flow_imbalance_weight",
                    &self.factors.flow_imbalance_weight,
                )?,
                trade_velocity_alpha: parse_decimal(
                    "factors.trade_velocity_alpha",
                    &self.factors.trade_velocity_alpha,
                )?,
                trade_velocity_burst_threshold: parse_decimal(
                    "factors.trade_velocity_burst_threshold",
                    &self.factors.trade_velocity_burst_threshold,
                )?,
                bbo_spread_vol_weight: parse_decimal(
                    "factors.bbo_spread_vol_weight",
                    &self.factors.bbo_spread_vol_weight,
                )?,
                inventory_risk_constant: parse_decimal(
                    "factors.inventory_risk_constant",
                    &self.factors.inventory_risk_constant,
                )?,
                inventory_skew_convexity: parse_decimal(
                    "factors.inventory_skew_convexity",
                    &self.factors.inventory_skew_convexity,
                )?,
                regime_min_dwell_secs: self.factors.regime_min_dwell_secs,
                cross_symbol_vol_weight: parse_decimal(
                    "factors.cross_symbol_vol_weight",
                    &self.factors.cross_symbol_vol_weight,
                )?,
                volatility_floor: parse_decimal(
                    "factors.volatility_floor",
                    &self.factors.volatility_floor,
                )?,
                ob_imbalance_weight: parse_decimal(
                    "factors.ob_imbalance_weight",
                    &self.factors.ob_imbalance_weight,
                )?,
                regime_intensity_alpha: parse_decimal(
                    "factors.regime_intensity_alpha",
                    &self.factors.regime_intensity_alpha,
                )?,
                ob_imbalance_depth: self.factors.ob_imbalance_depth,
                bbo_spread_cap_bps: parse_decimal(
                    "factors.bbo_spread_cap_bps",
                    &self.factors.bbo_spread_cap_bps,
                )?,
                flow_spike_pause_threshold: parse_decimal(
                    "factors.flow_spike_pause_threshold",
                    &self.factors.flow_spike_pause_threshold,
                )?,
                toxic_regime_block_new_positions_secs: self
                    .factors
                    .toxic_regime_block_new_positions_secs,
                toxic_regime_block_new_positions_intensity: parse_decimal(
                    "factors.toxic_regime_block_new_positions_intensity",
                    &self.factors.toxic_regime_block_new_positions_intensity,
                )?
                .clamp(Decimal::ZERO, Decimal::ONE),
                microprice_weight: parse_decimal(
                    "factors.microprice_weight",
                    &self.factors.microprice_weight,
                )?
                .clamp(Decimal::ZERO, Decimal::ONE),
                cex_reference_lean_weight: parse_decimal(
                    "factors.cex_reference_lean_weight",
                    &self.factors.cex_reference_lean_weight,
                )?,
                vpin_bucket_size: parse_decimal(
                    "factors.vpin_bucket_size",
                    &self.factors.vpin_bucket_size,
                )?,
                vpin_n_buckets: self.factors.vpin_n_buckets,
                vpin_widen_threshold: parse_decimal(
                    "factors.vpin_widen_threshold",
                    &self.factors.vpin_widen_threshold,
                )?,
            },
            hedging: ParsedHedgingConfig {
                hyperliquid_taker_fee_rate: parse_decimal(
                    "hedging.hyperliquid_taker_fee_rate",
                    &self.hedging.hyperliquid_taker_fee_rate,
                )?,
            },
            pairs: self
                .pairs
                .iter()
                .map(|pair| {
                    Ok(ParsedPairConfig {
                        symbol: pair.symbol.clone(),
                        max_position_base: parse_decimal(
                            &format!("pairs.{}.max_position_base", pair.symbol),
                            &pair.max_position_base,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        })
    }

    pub fn pairs_by_symbol(&self) -> HashMap<&str, &PairConfig> {
        self.pairs
            .iter()
            .map(|pair| (pair.symbol.as_str(), pair))
            .collect()
    }
}

fn parse_decimal(field: &str, raw: &str) -> Result<Decimal> {
    raw.parse::<Decimal>()
        .with_context(|| format!("invalid decimal for {field}: {raw}"))
}

#[derive(Clone, Debug)]
pub struct ParsedConfig {
    pub venue: ParsedVenueConfig,
    pub runtime: ParsedRuntimeConfig,
    pub model: ParsedModelConfig,
    pub risk: ParsedRiskConfig,
    pub factors: ParsedFactorConfig,
    pub hedging: ParsedHedgingConfig,
    pub pairs: Vec<ParsedPairConfig>,
}

impl ParsedConfig {
    pub fn pair(&self, symbol: &str) -> Option<&ParsedPairConfig> {
        self.pairs.iter().find(|pair| pair.symbol == symbol)
    }
}

#[derive(Clone, Debug)]
pub struct ParsedVenueConfig {
    pub maker_fee_rate: Decimal,
    pub taker_fee_rate: Decimal,
}

#[derive(Clone, Debug)]
pub struct ParsedRuntimeConfig {
    pub reconcile_interval_ms: u64,
    pub websocket_send_timeout_ms: u64,
    pub max_orderbook_depth: usize,
    pub restore_dry_run_state: bool,
}

#[derive(Clone, Debug)]
pub struct ParsedModelConfig {
    pub born_inf_bps: Decimal,
    pub born_sup_bps: Decimal,
    pub overnight_born_inf_bps: Decimal,
    pub overnight_start_hour_utc: u8,
    pub overnight_end_hour_utc: u8,
    pub v0: Decimal,
    pub mu: Decimal,
    pub sigma: Decimal,
    pub min_step_price: Decimal,
    pub min_step_volume: Decimal,
    pub min_trade_amount: Decimal,
    pub position_size_skew_weight: Decimal,
    pub position_spread_multiplier: Decimal,
    pub position_dead_zone: Decimal,
    pub price_sensitivity_threshold: Decimal,
    pub price_sensitivity_scaling_factor: Decimal,
    pub volume_sensitivity_threshold: Decimal,
    pub volatility_cut_threshold: Decimal,
    pub index_forward_buffer_bps: Decimal,
    pub max_spread_bps: Decimal,
    pub as_gamma: Decimal,
    pub as_kappa: Decimal,
    pub as_time_horizon: Decimal,
    pub inventory_lean_bps: Decimal,
    /// Hard cap on vol_per_cycle fed into the A-S formula, preventing BBO noise
    /// from inflating spreads while genuine volatility still widens them naturally.
    pub as_vol_cap: Decimal,
    /// Rolling window (seconds) for fill-rate asymmetry tracking.
    pub fill_rate_window_secs: Decimal,
    /// Ratio threshold that triggers fill-rate competitive skew.
    pub fill_rate_skew_threshold: Decimal,
    /// Extra competitiveness added on the slow-filling side (bps).
    pub fill_rate_competitive_bps: Decimal,
    /// Spread multiplier at VPIN = 1 (linear interpolation above vpin_widen_threshold).
    pub vpin_widen_multiplier: Decimal,
    /// Funding-rate lean weight.
    pub funding_lean_weight: Decimal,
    /// Seconds to widen the opposite side after a fill.
    pub post_fill_widen_secs: u64,
    /// Spread multiplier during the post-fill widen window.
    pub post_fill_widen_multiplier: Decimal,
    /// Use online kappa estimate when available.
    pub online_kappa: bool,
    /// Minimum quote cycles before online kappa is trusted.
    pub kappa_min_cycles: u64,
}

#[derive(Clone, Debug)]
pub struct ParsedRiskConfig {
    pub max_abs_position_usd: Decimal,
    pub max_symbol_position_usd: Decimal,
    pub max_drawdown_usd: Decimal,
    pub emergency_widening_bps: Decimal,
    pub emergency_skew_start: Decimal,
    pub emergency_skew_max: Decimal,
    pub max_correlated_position_usd: Decimal,
    pub circuit_breaker_cooldown_ms: u64,
    pub emergency_unwind_threshold: Decimal,
    pub emergency_unwind_cycles: usize,
    pub stale_position_timeout_secs: u64,
    pub stale_position_bid_hold_secs: u64,
    pub stale_position_ask_hold_secs: u64,
    pub stale_position_close_slippage_cap_bps: Decimal,
    pub stale_position_close_chunk_base: Decimal,
}

#[derive(Clone, Debug)]
pub struct ParsedFactorConfig {
    pub volatility_ewma_alpha: Decimal,
    pub volatility_spread_weight: Decimal,
    pub inventory_skew_weight: Decimal,
    pub volume_size_weight: Decimal,
    pub flow_imbalance_weight: Decimal,
    pub trade_velocity_alpha: Decimal,
    pub trade_velocity_burst_threshold: Decimal,
    pub bbo_spread_vol_weight: Decimal,
    pub inventory_risk_constant: Decimal,
    pub inventory_skew_convexity: Decimal,
    pub regime_min_dwell_secs: u64,
    pub cross_symbol_vol_weight: Decimal,
    pub volatility_floor: Decimal,
    /// How strongly ob_imbalance shifts the reservation price and asymmetric spread/size.
    /// 0 = disabled, 0.5 = moderate, 1.0 = full effect.
    pub ob_imbalance_weight: Decimal,
    /// EWMA alpha used to smooth regime intensity. 1 = immediate, lower = smoother.
    pub regime_intensity_alpha: Decimal,
    /// Number of top orderbook levels to sum when computing ob_imbalance (0 = all levels).
    pub ob_imbalance_depth: usize,
    /// Hard cap on BBO spread (in bps) before applying bbo_spread_vol_weight,
    /// preventing momentary wide spreads from inflating the volatility addon.
    pub bbo_spread_cap_bps: Decimal,
    /// |flow_direction| above this value pauses quoting for the symbol.
    /// 0 = disabled. Typical: 0.7.
    pub flow_spike_pause_threshold: Decimal,
    /// When TrendingToxic persists beyond this duration, do not open fresh positions.
    pub toxic_regime_block_new_positions_secs: u64,
    /// Minimum regime intensity required before persistent toxic-regime protection activates.
    pub toxic_regime_block_new_positions_intensity: Decimal,
    /// Blend weight for microprice vs raw mid [0, 1].
    pub microprice_weight: Decimal,
    /// Weight applied to (microprice - grvt_mid) when anchoring to CEX spot.
    pub cex_reference_lean_weight: Decimal,
    /// Volume per VPIN bucket. 0 = VPIN disabled.
    pub vpin_bucket_size: Decimal,
    /// Number of VPIN buckets in the rolling window.
    pub vpin_n_buckets: usize,
    /// VPIN threshold above which quotes widen.
    pub vpin_widen_threshold: Decimal,
}

#[derive(Clone, Debug)]
pub struct ParsedHedgingConfig {
    pub hyperliquid_taker_fee_rate: Decimal,
}

#[derive(Clone, Debug)]
pub struct ParsedPairConfig {
    pub symbol: String,
    pub max_position_base: Decimal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VenueConfig {
    #[serde(default)]
    pub kind: ExchangeKind,
    pub api_base_url: String,
    pub data_api_base_url: String,
    pub ws_market_url: String,
    pub ws_account_url: String,
    pub account_id: u64,
    pub api_key: String,
    pub private_key: String,
    pub auth_mode: HibachiAuthMode,
    pub user_agent: String,
    #[serde(default = "default_zero_string")]
    pub maker_fee_ratio: String,
    pub max_fee_rate: String,
    pub creation_deadline_ms: u64,
    pub price_multiplier: String,
    #[serde(default)]
    pub grvt_auth_base_url: String,
    #[serde(default)]
    pub grvt_market_data_base_url: String,
    #[serde(default)]
    pub grvt_trading_base_url: String,
    #[serde(default)]
    pub grvt_market_ws_url: String,
    #[serde(default)]
    pub grvt_trading_ws_url: String,
    #[serde(default)]
    pub grvt_account_id: String,
    #[serde(default)]
    pub grvt_sub_account_id: String,
    #[serde(default)]
    pub grvt_private_key: String,
    #[serde(default)]
    pub grvt_api_key: String,
    #[serde(default)]
    pub grvt_chain_id: String,
    #[serde(default)]
    pub grvt_builder_id: String,
    #[serde(default = "default_zero_string")]
    pub grvt_builder_fee: String,
    #[serde(default)]
    pub decibel_origin: String,
    #[serde(default)]
    pub decibel_account_address: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeKind {
    #[default]
    Hibachi,
    Grvt,
    Extended,
    Decibel,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HibachiAuthMode {
    ExchangeManaged,
    Trustless,
}

fn default_zero_string() -> String {
    "0".to_string()
}

fn default_websocket_send_timeout_ms() -> u64 {
    5_000
}

fn default_max_orderbook_depth() -> usize {
    100
}

fn default_restore_dry_run_state() -> bool {
    true
}

fn default_max_spread_bps() -> String {
    "100".to_string()
}

fn default_circuit_breaker_cooldown_ms() -> u64 {
    30_000
}

fn default_emergency_unwind_threshold() -> String {
    "0.8".to_string()
}

fn default_emergency_unwind_cycles() -> usize {
    5
}

fn default_stale_position_timeout_secs() -> u64 {
    10 * 60
}

fn default_stale_position_bid_hold_secs() -> u64 {
    5 * 60
}

fn default_stale_position_ask_hold_secs() -> u64 {
    60
}

fn default_stale_position_close_slippage_cap_bps() -> String {
    "50".to_string()
}

fn default_empty_quote_alert_cycles() -> usize {
    5
}

fn default_inventory_skew_convexity() -> String {
    "1.0".to_string()
}

fn default_regime_min_dwell_secs() -> u64 {
    3
}

fn default_ob_imbalance_depth() -> usize {
    10
}

fn default_position_size_skew_weight() -> String {
    "0.5".to_string()
}

fn default_as_gamma() -> String {
    "0.3".to_string()
}

fn default_as_kappa() -> String {
    "500.0".to_string()
}

fn default_as_time_horizon() -> String {
    "0.5".to_string()
}

fn default_inventory_lean_bps() -> String {
    "10.0".to_string()
}

fn default_as_vol_cap() -> String {
    "0.010".to_string()
}

fn default_bbo_spread_cap_bps() -> String {
    "20".to_string()
}

fn default_flow_spike_pause_threshold() -> String {
    "0".to_string() // disabled by default; set to e.g. "0.7" to enable
}

fn default_regime_intensity_alpha() -> String {
    "0.2".to_string()
}

fn default_toxic_regime_block_new_positions_secs() -> u64 {
    30 * 60
}

fn default_toxic_regime_block_new_positions_intensity() -> String {
    "0.7".to_string()
}

fn default_hyperliquid_taker_fee_rate() -> String {
    "0.000432".to_string()
}

fn default_microprice_weight() -> String {
    "1.0".to_string()
}

fn default_vpin_bucket_size() -> String {
    "0".to_string()
}

fn default_vpin_n_buckets() -> usize {
    20
}

fn default_fill_rate_window_secs() -> String {
    "300".to_string()
}

fn default_fill_rate_skew_threshold() -> String {
    "3.0".to_string()
}

fn default_one_string() -> String {
    "1.0".to_string()
}

fn default_false_string() -> String {
    "false".to_string()
}

fn default_kappa_min_cycles() -> String {
    "100".to_string()
}

fn default_zero_u8() -> u8 {
    0
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RuntimeConfig {
    pub dry_run: bool,
    pub reconcile_interval_ms: u64,
    pub generation_min_interval_ms: u64,
    pub stale_market_data_ms: i64,
    #[serde(default = "default_websocket_send_timeout_ms")]
    pub websocket_send_timeout_ms: u64,
    #[serde(default = "default_max_orderbook_depth")]
    pub max_orderbook_depth: usize,
    #[serde(default = "default_restore_dry_run_state")]
    pub restore_dry_run_state: bool,
    /// Number of consecutive empty-quote cycles (while holding a position) before
    /// emitting a Telegram alert.
    #[serde(default = "default_empty_quote_alert_cycles")]
    pub empty_quote_alert_cycles: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetworkConfig {
    pub private_rest_min_interval_ms: u64,
    pub retry_max_attempts: usize,
    pub retry_initial_backoff_ms: u64,
    pub retry_max_backoff_ms: u64,
    pub websocket_reconnect_backoff_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ModelConfig {
    pub born_inf_bps: String,
    pub born_sup_bps: String,
    #[serde(default = "default_zero_string")]
    pub overnight_born_inf_bps: String,
    #[serde(default = "default_zero_u8")]
    pub overnight_start_hour_utc: u8,
    #[serde(default = "default_zero_u8")]
    pub overnight_end_hour_utc: u8,
    pub v0: String,
    pub mu: String,
    pub sigma: String,
    pub n_points: usize,
    pub min_step_price: String,
    pub min_step_volume: String,
    pub min_trade_amount: String,
    /// How strongly a large position skews order sizes towards the unwind side. (S2)
    #[serde(default = "default_position_size_skew_weight")]
    pub position_size_skew_weight: String,
    pub position_spread_multiplier: String,
    pub position_dead_zone: String,
    pub price_sensitivity_threshold: String,
    pub price_sensitivity_scaling_factor: String,
    pub volume_sensitivity_threshold: String,
    pub volatility_cut_threshold: String,
    pub index_forward_buffer_bps: String,
    #[serde(default = "default_max_spread_bps")]
    pub max_spread_bps: String,
    pub prevent_spread_crossing: bool,
    pub cancel_orders_crossing_mid: bool,
    /// Avellaneda-Stoikov risk aversion parameter (γ). Higher = wider spread.
    #[serde(default = "default_as_gamma")]
    pub as_gamma: String,
    /// A-S market order arrival rate approximation (κ). Higher = tighter spread.
    #[serde(default = "default_as_kappa")]
    pub as_kappa: String,
    /// A-S inventory time horizon in hours.
    #[serde(default = "default_as_time_horizon")]
    pub as_time_horizon: String,
    /// Additional inventory lean in bps applied at max position (pos_ratio = 1).
    /// Shifts the reservation price by this amount linearly with pos_ratio.
    #[serde(default = "default_inventory_lean_bps")]
    pub inventory_lean_bps: String,
    /// Hard cap on vol_per_cycle fed into the A-S formula (e.g. "0.010" = 1%).
    #[serde(default = "default_as_vol_cap")]
    pub as_vol_cap: String,
    /// Rolling window (seconds) for fill-rate asymmetry tracking.
    #[serde(default = "default_fill_rate_window_secs")]
    pub fill_rate_window_secs: String,
    /// Ratio of bid/ask fill rate that triggers competitive skew (e.g. "3.0").
    #[serde(default = "default_fill_rate_skew_threshold")]
    pub fill_rate_skew_threshold: String,
    /// Extra spread competitiveness added on the slow-filling side when skew triggers (bps).
    #[serde(default = "default_zero_string")]
    pub fill_rate_competitive_bps: String,
    /// Spread multiplier applied when VPIN ≥ vpin_widen_threshold (at VPIN=1). 1 = disabled.
    #[serde(default = "default_one_string")]
    pub vpin_widen_multiplier: String,
    /// Weight for funding-rate lean. At weight=1 the funding rate (bps/hour) directly shifts
    /// the reservation price. 0 = disabled.
    #[serde(default = "default_zero_string")]
    pub funding_lean_weight: String,
    /// Seconds to widen the opposite side after a fill. 0 = disabled.
    #[serde(default = "default_zero_string")]
    pub post_fill_widen_secs: String,
    /// Spread multiplier on the post-fill side during the widen window. 1 = no widen.
    #[serde(default = "default_one_string")]
    pub post_fill_widen_multiplier: String,
    /// Enable online kappa estimation from empirical fill probability.
    /// When enabled, replaces as_kappa once enough cycles are observed.
    #[serde(default = "default_false_string")]
    pub online_kappa: String,
    /// Minimum quote cycles before online kappa estimate is trusted.
    #[serde(default = "default_kappa_min_cycles")]
    pub kappa_min_cycles: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RiskConfig {
    pub max_abs_position_usd: String,
    pub max_symbol_position_usd: String,
    pub max_drawdown_usd: String,
    pub max_open_orders: usize,
    pub emergency_widening_bps: String,
    #[serde(default = "default_zero_string")]
    pub emergency_skew_start: String,
    #[serde(default = "default_zero_string")]
    pub emergency_skew_max: String,
    #[serde(default = "default_zero_string")]
    pub max_correlated_position_usd: String,
    #[serde(default = "default_circuit_breaker_cooldown_ms")]
    pub circuit_breaker_cooldown_ms: u64,
    /// Fraction of max_position_base at which emergency-unwind mode triggers.
    #[serde(default = "default_emergency_unwind_threshold")]
    pub emergency_unwind_threshold: String,
    /// Consecutive empty-unwind cycles before emergency mode activates.
    #[serde(default = "default_emergency_unwind_cycles")]
    pub emergency_unwind_cycles: usize,
    /// Age in seconds after which stale positions switch to IOC limit-close logic.
    #[serde(default = "default_stale_position_timeout_secs")]
    pub stale_position_timeout_secs: u64,
    /// Side-specific stale-position timeout for short inventory (close via bids).
    #[serde(default = "default_stale_position_bid_hold_secs")]
    pub stale_position_bid_hold_secs: u64,
    /// Side-specific stale-position timeout for long inventory (close via asks).
    #[serde(default = "default_stale_position_ask_hold_secs")]
    pub stale_position_ask_hold_secs: u64,
    /// Slippage cap in bps applied to IOC stale closes versus mid/reference price.
    #[serde(default = "default_stale_position_close_slippage_cap_bps")]
    pub stale_position_close_slippage_cap_bps: String,
    /// Max base-size per stale close attempt. 0 = full position.
    #[serde(default = "default_zero_string")]
    pub stale_position_close_chunk_base: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FactorConfig {
    pub volume_window_secs: i64,
    pub n_trade_window_secs: i64,
    pub n_trade_quote_threshold: usize,
    pub volatility_ewma_alpha: String,
    pub volatility_spread_weight: String,
    pub inventory_skew_weight: String,
    pub volume_size_weight: String,
    #[serde(default = "default_zero_string")]
    pub flow_imbalance_weight: String,
    #[serde(default = "default_zero_string")]
    pub trade_velocity_alpha: String,
    #[serde(default = "default_zero_string")]
    pub trade_velocity_burst_threshold: String,
    #[serde(default = "default_zero_string")]
    pub bbo_spread_vol_weight: String,
    #[serde(default = "default_zero_string")]
    pub inventory_risk_constant: String,
    /// Convexity of the inventory skew penalty; higher = grows faster near limits. (S3)
    #[serde(default = "default_inventory_skew_convexity")]
    pub inventory_skew_convexity: String,
    /// Minimum seconds in a regime before allowing a regime transition. (S5)
    #[serde(default = "default_regime_min_dwell_secs")]
    pub regime_min_dwell_secs: u64,
    #[serde(default = "default_zero_string")]
    pub cross_symbol_vol_weight: String,
    #[serde(default = "default_zero_string")]
    pub volatility_floor: String,
    #[serde(default = "default_zero_string")]
    pub ob_imbalance_weight: String,
    #[serde(default = "default_regime_intensity_alpha")]
    pub regime_intensity_alpha: String,
    #[serde(default = "default_ob_imbalance_depth")]
    pub ob_imbalance_depth: usize,
    /// Hard cap on BBO spread (bps) before applying bbo_spread_vol_weight.
    #[serde(default = "default_bbo_spread_cap_bps")]
    pub bbo_spread_cap_bps: String,
    /// |flow_direction| above this threshold pauses quoting. 0 = disabled.
    #[serde(default = "default_flow_spike_pause_threshold")]
    pub flow_spike_pause_threshold: String,
    /// Duration in seconds before persistent TrendingToxic blocks new positions.
    #[serde(default = "default_toxic_regime_block_new_positions_secs")]
    pub toxic_regime_block_new_positions_secs: u64,
    /// Minimum TrendingToxic intensity before the persistent block activates.
    #[serde(default = "default_toxic_regime_block_new_positions_intensity")]
    pub toxic_regime_block_new_positions_intensity: String,
    /// Blend weight for microprice vs raw mid [0, 1]. 1 = full microprice, 0 = raw mid.
    /// Requires BBO size data from the venue (GRVT v1.mini.d `bq`/`aq`).
    #[serde(default = "default_microprice_weight")]
    pub microprice_weight: String,
    /// Weight applied to (microprice - grvt_mid) added to CEX spot when price_source=spot.
    /// 0 = pure CEX price, 1 = CEX + full DEX book-pressure lean.
    #[serde(default = "default_zero_string")]
    pub cex_reference_lean_weight: String,
    /// Volume per VPIN bucket (in base currency). 0 = disabled.
    #[serde(default = "default_vpin_bucket_size")]
    pub vpin_bucket_size: String,
    /// Number of completed VPIN buckets in the rolling window.
    #[serde(default = "default_vpin_n_buckets")]
    pub vpin_n_buckets: usize,
    /// VPIN level above which spreads widen by `vpin_widen_multiplier`. 0 = disabled.
    #[serde(default = "default_zero_string")]
    pub vpin_widen_threshold: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HedgingConfig {
    #[serde(default = "default_hyperliquid_taker_fee_rate")]
    pub hyperliquid_taker_fee_rate: String,
}

impl Default for HedgingConfig {
    fn default() -> Self {
        Self {
            hyperliquid_taker_fee_rate: default_hyperliquid_taker_fee_rate(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageConfig {
    pub enabled: bool,
    pub db_path: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PairConfig {
    pub symbol: String,
    pub enabled: bool,
    pub max_position_base: String,
    pub price_source: PriceSource,
    pub post_only: bool,
    pub service_on: bool,
    pub side_filter: SideFilter,
    /// Optional Binance futures stream name (lowercase base, e.g. `"resolv"` for
    /// the `resolvusdt@markPrice@1s` stream).  When set, Binance mark prices are
    /// streamed and injected as `MarketEvent::SpotPrice` for this symbol.
    #[serde(default)]
    pub binance_symbol: Option<String>,
    /// Optional Hyperliquid perp coin symbol (e.g. `"CRV"`). When set, the bot
    /// subscribes to Hyperliquid BBO updates for cross-venue basis monitoring
    /// and dry-run hedge simulation only.
    #[serde(default)]
    pub hyperliquid_symbol: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SideFilter {
    Both,
    BidOnly,
    AskOnly,
    None,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceSource {
    Mid,
    Mark,
    Spot,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: String,
    pub enabled: bool,
}
