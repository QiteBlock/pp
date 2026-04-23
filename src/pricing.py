from __future__ import annotations

from dataclasses import dataclass

from .signals import PriceSignal


@dataclass
class PricingConfig:
    min_half_spread: float
    max_half_spread: float
    inventory_skew_per_share: float
    resolution_risk_multiplier: float
    volatility_multiplier: float
    book_spread_multiplier: float
    probability_edge_multiplier: float
    size_per_order: float
    quote_improvement_ticks: int


@dataclass
class Quote:
    bid: float
    ask: float
    fair_value: float
    half_spread: float
    size: float


def clamp_probability(price: float) -> float:
    return min(max(price, 0.01), 0.99)


def derive_fair_value(signal: PriceSignal) -> float:
    if signal.midpoint is not None:
        return clamp_probability(signal.midpoint)
    if signal.last_trade is not None:
        return clamp_probability(signal.last_trade)
    return 0.5


def compute_half_spread(config: PricingConfig, signal: PriceSignal, inventory_bias: float, book_spread: float | None, fair_value: float) -> float:
    live_half_spread = (book_spread / 2.0) if book_spread is not None and book_spread > 0 else config.min_half_spread
    book_buffer = live_half_spread * config.book_spread_multiplier
    tail_risk_buffer = min(fair_value, 1.0 - fair_value) * config.probability_edge_multiplier
    resolution_buffer = min(fair_value, 1.0 - fair_value) * config.resolution_risk_multiplier / max(signal.time_to_resolution_days, 1.0)
    volatility_buffer = signal.realized_volatility * config.volatility_multiplier
    inventory_buffer = abs(inventory_bias) * config.inventory_skew_per_share
    raw = book_buffer + tail_risk_buffer + resolution_buffer + volatility_buffer + inventory_buffer
    return min(max(raw, config.min_half_spread), config.max_half_spread)


def build_yes_quote(config: PricingConfig, signal: PriceSignal, inventory_bias: float, tick_size: float, book_spread: float | None = None) -> Quote:
    fair_value = derive_fair_value(signal)
    half_spread = compute_half_spread(config, signal, inventory_bias, book_spread, fair_value)
    skew = inventory_bias * config.inventory_skew_per_share
    bid = clamp_probability(fair_value - half_spread - skew)
    ask = clamp_probability(fair_value + half_spread - skew)
    if ask <= bid:
        ask = clamp_probability(bid + max(tick_size, 0.01))
    return Quote(
        bid=round_to_tick(bid, tick_size, down=True),
        ask=round_to_tick(ask, tick_size, down=False),
        fair_value=fair_value,
        half_spread=half_spread,
        size=config.size_per_order,
    )


def round_to_tick(price: float, tick_size: float, down: bool) -> float:
    tick = max(tick_size, 0.01)
    scaled = price / tick
    rounded = int(scaled) if down else int(-(-scaled // 1))
    return clamp_probability(round(rounded * tick, 4))
