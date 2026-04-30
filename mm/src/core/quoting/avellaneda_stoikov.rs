use crate::{
    config::{ModelConfig, PairConfig, ParsedConfig, SideFilter},
    core::analytics::tracker::FillTracker,
    domain::{FactorSnapshot, QuoteIntent, Side},
};
use chrono::{Timelike, Utc};
use rust_decimal::{Decimal, MathematicalOps};

/// Generate quote intents for one symbol.
///
/// Pricing uses the Avellaneda-Stoikov model:
///   reservation price r = mid - q·γ·σ²·T
///   optimal spread     δ = γ·σ²·T + (2/γ)·ln(1 + γ/κ)
///
/// where q is the normalised inventory position (pos_ratio), σ is realised
/// volatility from factors, T is the configured time horizon in hours, γ is
/// risk-aversion, and κ is the market-order arrival rate proxy.
///
/// The A-S bid/ask give a dynamic minimum spread that widens automatically
/// with volatility.  Level 0 is additionally clamped to be no worse than the
/// live BBO (S1 anchor), and deeper levels step away from that anchor.
pub fn generate_quotes(
    model: &ModelConfig,
    parsed: &ParsedConfig,
    pair: &PairConfig,
    factors: &FactorSnapshot,
    fill_tracker: &FillTracker,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    pos_ratio: Decimal,
) -> Vec<QuoteIntent> {
    if !pair.service_on || pair.side_filter == SideFilter::None || model.n_points == 0 {
        return Vec::new();
    }

    let v0 = if factors.regime == crate::domain::MarketRegime::VolatileBalanced {
        parsed.model.v0 * Decimal::new(15, 1)
    } else {
        parsed.model.v0
    };
    let effective_born_inf_bps = current_born_inf_bps(parsed, factors.regime_intensity);
    let mu = parsed.model.mu;
    let gauss_sigma = parsed.model.sigma.max(Decimal::new(1, 6));
    let dead_zone = parsed.model.position_dead_zone.abs();
    let spread_multiplier = parsed.model.position_spread_multiplier;
    // For post-only quotes, the relevant execution-cost floor is maker-only.
    // Using maker+taker here makes zero-maker venues artificially uncompetitive.
    let fee_floor_rate = if pair.post_only {
        parsed.venue.maker_fee_rate
    } else {
        parsed.venue.maker_fee_rate + parsed.venue.taker_fee_rate
    };
    let fee_floor_bps = fee_floor_rate * Decimal::from(10_000u64);
    let flow_spike_widen_mult = factors.flow_spike_widen_multiplier.max(Decimal::ONE);

    // A-S parameters
    let gamma = parsed.model.as_gamma.max(Decimal::new(1, 6));
    // Use online kappa estimate when available and enabled; fall back to config.
    let kappa = factors
        .kappa_estimate
        .unwrap_or(parsed.model.as_kappa)
        .max(Decimal::new(1, 6));
    // Time horizon in hours.
    let time_horizon = parsed.model.as_time_horizon.max(Decimal::new(1, 6));

    // Annualise σ: raw_volatility is per-cycle log-return std dev (EWMA).
    // At 1s reconcile interval there are 3600 cycles/hour; σ_hour = σ_cycle * sqrt(3600) = σ_cycle * 60.
    // Using sqrt(3600) = 60 in integer arithmetic.
    // B2: cap vol before it enters A-S to prevent BBO noise from inflating spread.
    let vol_per_cycle = factors
        .raw_volatility
        .max(parsed.factors.volatility_floor)
        .min(parsed.model.as_vol_cap);
    let cycles_per_hour = Decimal::from(3600u64); // 1s cycles; adjust if reconcile_interval changes
    let vol_per_hour = vol_per_cycle * cycles_per_hour.sqrt().unwrap_or(Decimal::from(60u64));

    // A-S: γ·σ_hour²·T
    let gamma_sigma2_t = gamma * vol_per_hour * vol_per_hour * time_horizon;

    // A-S optimal half-spread in price fraction:
    //   δ/2 = (γ·σ²·T)/2 + (1/γ)·ln(1 + γ/κ)
    // κ is calibrated in price-fraction space. Default κ=1.5 → ln(1+0.05/1.5)/0.05 ≈ 3.2bps
    // — a meaningful minimum that reacts to vol.
    let ln_term = {
        let arg = Decimal::ONE + gamma / kappa;
        arg.ln()
    };
    let as_half_spread_frac_raw = gamma_sigma2_t / Decimal::TWO + ln_term / gamma;

    // Clamp to [fee_floor/2, max_spread/2] in bps, then convert back to fraction for pricing.
    let min_half_spread_bps =
        (fee_floor_bps / Decimal::TWO).max(effective_born_inf_bps / Decimal::TWO);
    let as_half_spread_bps = (as_half_spread_frac_raw * Decimal::from(10_000u64))
        .max(min_half_spread_bps)
        .min(parsed.model.max_spread_bps / Decimal::TWO);
    // Use the clamped fraction for all price calculations.
    let as_half_spread_frac = as_half_spread_bps / Decimal::from(10_000u64);

    // A-S reservation price: mid shifted by A-S term + inventory lean + ob_imbalance lean
    // + funding lean (positive = lean ask, i.e. subtract from mid).
    let mid = factors.price_index;
    let inventory_lean_frac = factors.inventory_lean_bps / Decimal::from(10_000u64);
    let ob_weight = parsed.factors.ob_imbalance_weight;
    // ob_imbalance_lean: at full imbalance (±1) and weight=1, shifts mid by ±ob_weight bps.
    let ob_lean_frac = factors.ob_imbalance * ob_weight / Decimal::from(10_000u64);
    // Funding lean: positive funding_lean shifts reservation down (lean short = offer more).
    let funding_lean_frac = factors.funding_lean / Decimal::from(10_000u64);
    let as_reservation_price = mid
        * (Decimal::ONE - pos_ratio * gamma_sigma2_t - pos_ratio * inventory_lean_frac
            + ob_lean_frac
            - funding_lean_frac);

    // S6: volume_imbalance is purely a size scalar (>= 0).
    let volume_factor = (Decimal::ONE + factors.volume_imbalance).max(Decimal::ZERO);

    let position_signal = if factors.inventory_skew.abs() < dead_zone {
        Decimal::ZERO
    } else {
        factors.inventory_skew
    };
    // Cap at 3× so the accumulating side never gets pushed more than 3× spread away from mid.
    let bid_skew = (Decimal::ONE + position_signal * spread_multiplier)
        .max(Decimal::ZERO)
        .min(Decimal::new(3, 0));
    let ask_skew = (Decimal::ONE - position_signal * spread_multiplier)
        .max(Decimal::ZERO)
        .min(Decimal::new(3, 0));

    // OB imbalance: asymmetric spread and size adjustments.
    // Positive ob_imbalance = bid-heavy = ask side is about to be swept:
    //   → widen ask spread (protect from adverse selection)
    //   → tighten bid spread (lean into the upward pressure, be more competitive)
    //   → reduce ask size (don't give away cheap asks into bid sweep)
    //   → boost bid size (more aggressive on the safe side)
    // Negative ob_imbalance = ask-heavy: mirror image.
    let ob = factors.ob_imbalance * ob_weight.min(Decimal::ONE);
    // ob_spread_multiplier ∈ [0.5, 1.5] at full imbalance.
    let ob_ask_spread_mult = (Decimal::ONE + ob * Decimal::new(5, 1))
        .max(Decimal::new(5, 1))
        .min(Decimal::new(15, 1));
    let ob_bid_spread_mult = (Decimal::ONE - ob * Decimal::new(5, 1))
        .max(Decimal::new(5, 1))
        .min(Decimal::new(15, 1));
    // ob_size_multiplier ∈ [0.5, 1.5] at full imbalance.
    let ob_ask_size_mult = (Decimal::ONE - ob * Decimal::new(2, 0))
        .max(Decimal::new(2, 1))
        .min(Decimal::new(3, 0));
    let ob_bid_size_mult = (Decimal::ONE + ob * Decimal::new(2, 0))
        .max(Decimal::new(2, 1))
        .min(Decimal::new(3, 0));

    // S5: Continuous regime scaling.
    let intensity = factors.regime_intensity;
    let regime_spread_multiplier = Decimal::new(9, 1) + Decimal::new(9, 1) * intensity;
    let regime_size_multiplier = Decimal::new(12, 1) - Decimal::new(8, 1) * intensity;

    // VPIN spread widening: linearly interpolate between 1× and vpin_widen_multiplier.
    let vpin_threshold = parsed.factors.vpin_widen_threshold;
    let vpin_widen_mult = if vpin_threshold > Decimal::ZERO && factors.vpin > vpin_threshold {
        let excess = (factors.vpin - vpin_threshold)
            / (Decimal::ONE - vpin_threshold).max(Decimal::new(1, 6));
        let mult = parsed.model.vpin_widen_multiplier;
        Decimal::ONE + (mult - Decimal::ONE) * excess.min(Decimal::ONE)
    } else {
        Decimal::ONE
    };
    let bid_widen = factors.post_fill_widen_bid;
    let ask_widen = factors.post_fill_widen_ask;

    // Fill-rate skew: when bid fills 3× faster than ask, add extra competitiveness to ask
    // (tighten it). Skew > 0 = bid faster → make ask more competitive (lower price).
    // We apply it as an extra price offset on the slow-filling side at level 0.
    let fill_rate_comp_frac = parsed.model.fill_rate_competitive_bps / Decimal::from(10_000u64);
    let fill_rate_skew = factors.fill_rate_skew; // signed [-1, 1]

    // S4: signed flow direction for toxic-flow detection.
    let flow_dir = factors.flow_direction;
    let toxic_reduction =
        (Decimal::ONE - Decimal::new(6, 1) * flow_dir.abs()).max(Decimal::new(4, 1));
    let safe_boost = (Decimal::ONE + Decimal::new(3, 1) * flow_dir.abs()).min(Decimal::new(15, 1));

    // Level spacing always uses born_inf/sup_bps so the grid is independent of A-S.
    // A-S only sets the anchor price at level 0; deeper levels step from that anchor.
    // Enforce a minimum 2 bps step so levels don't cluster together.
    let min_step_bps = Decimal::TWO;
    let step = if model.n_points <= 1 {
        Decimal::ZERO
    } else {
        let raw_step = (parsed.model.born_sup_bps - parsed.model.born_inf_bps)
            / Decimal::from((model.n_points - 1) as u64);
        raw_step.max(min_step_bps)
    };

    // A-S bid/ask from reservation price.
    let as_bid = as_reservation_price * (Decimal::ONE - as_half_spread_frac * bid_skew);
    let as_ask = as_reservation_price * (Decimal::ONE + as_half_spread_frac * ask_skew);

    // S1: Level 0 anchors to BBO, but no worse than A-S theoretical price.
    // If live BBO exists, use it clamped to A-S. If no BBO, fall back to A-S.
    let level0_unclamped_bps = (effective_born_inf_bps + factors.volatility)
        * regime_spread_multiplier
        * vpin_widen_mult
        * flow_spike_widen_mult;
    let level0_spread_bps = level0_unclamped_bps
        .max(fee_floor_bps)
        .min(parsed.model.max_spread_bps);
    let level0_half_spread_frac = level0_spread_bps / Decimal::from(20_000u64);
    let level0_bid_candidate = as_reservation_price
        * (Decimal::ONE
            - level0_half_spread_frac * Decimal::TWO * bid_skew * ob_bid_spread_mult * bid_widen);
    let level0_ask_candidate = as_reservation_price
        * (Decimal::ONE
            + level0_half_spread_frac * Decimal::TWO * ask_skew * ob_ask_spread_mult * ask_widen);
    let anchor_bid_base = as_bid.min(level0_bid_candidate);
    let anchor_ask_base = as_ask.max(level0_ask_candidate);
    let anchor_bid = match best_bid {
        Some(bb) => bb.min(anchor_bid_base),
        None => anchor_bid_base,
    };
    let anchor_ask = match best_ask {
        Some(ba) => ba.max(anchor_ask_base),
        None => anchor_ask_base,
    };

    // S2+P3: size asymmetry: inventory + flow direction.
    let size_skew_weight = parsed.model.position_size_skew_weight;
    let size_skew = (pos_ratio - flow_dir * Decimal::new(5, 1)) * size_skew_weight;
    let bid_size_skew = (Decimal::ONE - size_skew).max(Decimal::new(2, 1));
    let ask_size_skew = (Decimal::ONE + size_skew).max(Decimal::new(2, 1));

    let mut quotes = Vec::with_capacity(model.n_points * 2);
    for level in 0..model.n_points {
        let level_decimal = Decimal::from(level as u64);

        // Spread for this level: independent grid from born_inf to born_sup.
        // Apply regime + VPIN multipliers first, then clamp to fee floor.
        let unclamped_bps = (effective_born_inf_bps + factors.volatility + step * level_decimal)
            * regime_spread_multiplier
            * vpin_widen_mult
            * flow_spike_widen_mult;
        let level_spread_bps = unclamped_bps
            .max(fee_floor_bps)
            .min(parsed.model.max_spread_bps);
        let level_half_spread_frac = level_spread_bps / Decimal::from(20_000u64); // half-spread

        // Fill-rate competitive skew: when bid fills faster (skew > 0) → lower ask price.
        // Adjustment only at level 0 (anchor); deeper levels inherit via spread.
        let fr_ask_adj = if level == 0 {
            mid * fill_rate_comp_frac * fill_rate_skew.max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        let fr_bid_adj = if level == 0 {
            mid * fill_rate_comp_frac * (-fill_rate_skew).max(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };

        // S1: Level 0 anchors to BBO (clamped to A-S); deeper levels step from anchor.
        // OB imbalance multipliers widen the swept side and tighten the safe side.
        let (bid_price, ask_price) = if level == 0 {
            // Fill-rate: move toward mid on slow-filling side.
            (anchor_bid + fr_bid_adj, anchor_ask - fr_ask_adj)
        } else {
            let bid_spread =
                level_half_spread_frac * Decimal::TWO * bid_skew * ob_bid_spread_mult * bid_widen;
            let ask_spread =
                level_half_spread_frac * Decimal::TWO * ask_skew * ob_ask_spread_mult * ask_widen;
            (
                anchor_bid * (Decimal::ONE - bid_spread),
                anchor_ask * (Decimal::ONE + ask_spread),
            )
        };

        let weight = gaussian(level_decimal, mu, gauss_sigma);
        let base_size = (weight * v0 * volume_factor * regime_size_multiplier).max(Decimal::ZERO);
        let bid_fill_multiplier =
            fill_tracker.level_volume_multiplier(&pair.symbol, Side::Bid, level);
        let ask_fill_multiplier =
            fill_tracker.level_volume_multiplier(&pair.symbol, Side::Ask, level);

        let mut bid_size =
            (base_size * bid_fill_multiplier * bid_size_skew * ob_bid_size_mult).max(Decimal::ZERO);
        let mut ask_size =
            (base_size * ask_fill_multiplier * ask_size_skew * ob_ask_size_mult).max(Decimal::ZERO);

        // S4: graduated toxic-flow sided reduction / safe-side boost.
        if flow_dir > Decimal::ZERO {
            ask_size *= toxic_reduction;
            bid_size *= safe_boost;
        } else if flow_dir < Decimal::ZERO {
            bid_size *= toxic_reduction;
            ask_size *= safe_boost;
        }

        if matches!(pair.side_filter, SideFilter::Both | SideFilter::BidOnly) {
            quotes.push(QuoteIntent {
                symbol: pair.symbol.clone(),
                level_index: level,
                side: Side::Bid,
                price: bid_price,
                quantity: bid_size,
                post_only: pair.post_only,
            });
        }
        if matches!(pair.side_filter, SideFilter::Both | SideFilter::AskOnly) {
            quotes.push(QuoteIntent {
                symbol: pair.symbol.clone(),
                level_index: level,
                side: Side::Ask,
                price: ask_price,
                quantity: ask_size,
                post_only: pair.post_only,
            });
        }
    }

    quotes
}

fn current_born_inf_bps(parsed: &ParsedConfig, regime_intensity: Decimal) -> Decimal {
    let overnight_floor = parsed.model.overnight_born_inf_bps;
    let base_floor = if overnight_floor <= Decimal::ZERO {
        parsed.model.born_inf_bps
    } else {
        let start = parsed.model.overnight_start_hour_utc;
        let end = parsed.model.overnight_end_hour_utc;
        if start == end {
            parsed.model.born_inf_bps
        } else {
            let hour = Utc::now().hour() as u8;
            let in_window = if start < end {
                hour >= start && hour < end
            } else {
                hour >= start || hour < end
            };
            if in_window {
                parsed.model.born_inf_bps.max(overnight_floor)
            } else {
                parsed.model.born_inf_bps
            }
        }
    };
    let intensity = regime_intensity.clamp(Decimal::ZERO, Decimal::ONE);
    base_floor * (Decimal::ONE + intensity * Decimal::TWO)
}

pub fn factor_spread_addon(parsed: &ParsedConfig, volatility: Decimal) -> Decimal {
    volatility * parsed.factors.volatility_spread_weight
}

fn gaussian(x: Decimal, mu: Decimal, sigma: Decimal) -> Decimal {
    let exponent = -((x - mu) * (x - mu)) / (Decimal::from(2u64) * sigma * sigma);
    exponent.exp()
}
