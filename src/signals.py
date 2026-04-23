from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional


@dataclass
class PriceSignal:
    midpoint: Optional[float]
    last_trade: Optional[float]
    realized_volatility: float
    time_to_resolution_days: float


def compute_realized_volatility(prices: Iterable[float]) -> float:
    values = [price for price in prices if price is not None]
    if len(values) < 2:
        return 0.0
    absolute_moves = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    return sum(absolute_moves) / len(absolute_moves)


def compute_time_to_resolution_days(end_date: Optional[datetime]) -> float:
    if end_date is None:
        return 365.0
    now = datetime.now(timezone.utc)
    delta = end_date - now
    return max(delta.total_seconds() / 86400.0, 0.0)


def build_signal(midpoint: Optional[float], last_trade: Optional[float], recent_prices: List[float], end_date: Optional[datetime]) -> PriceSignal:
    return PriceSignal(
        midpoint=midpoint,
        last_trade=last_trade,
        realized_volatility=compute_realized_volatility(recent_prices),
        time_to_resolution_days=compute_time_to_resolution_days(end_date),
    )
