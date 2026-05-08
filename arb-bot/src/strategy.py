"""Pure decision logic for pair trading."""

from __future__ import annotations

from dataclasses import dataclass

from venues import Quote


@dataclass(frozen=True)
class LegSpec:
    venue: str
    symbol: str
    fee_bps_taker: float
    fee_bps_maker: float


@dataclass(frozen=True)
class PairSpec:
    name: str
    long_leg: LegSpec
    short_leg: LegSpec
    notional_per_trade_usd: float
    entry_threshold_bps: float
    exit_threshold_bps: float
    stop_loss_bps: float
    max_hold_secs: float
    max_position_usd: float


@dataclass
class Position:
    qty: float = 0.0
    long_avg_entry: float = 0.0
    short_avg_entry: float = 0.0
    realized_bps_pnl: float = 0.0
    opened_at_monotonic: float = 0.0
    entry_edge_bps: float = 0.0


def current_notional_usd(pos: Position) -> float:
    return abs(pos.qty) * pos.long_avg_entry


def executable_entry_bps(
    long_q: Quote,
    short_q: Quote,
    long_fee_bps: float,
    short_fee_bps: float,
) -> float:
    if long_q.ask <= 0 or short_q.bid <= 0:
        return float("-inf")
    gross = (short_q.bid - long_q.ask) / long_q.ask * 10_000.0
    return gross - long_fee_bps - short_fee_bps


def executable_exit_bps(
    pos: Position,
    long_q: Quote,
    short_q: Quote,
    long_fee_bps: float,
    short_fee_bps: float,
) -> float:
    if pos.qty <= 0 or pos.long_avg_entry <= 0 or pos.short_avg_entry <= 0:
        return float("-inf")
    if long_q.bid <= 0 or short_q.ask <= 0:
        return float("-inf")

    long_bps = (long_q.bid - pos.long_avg_entry) / pos.long_avg_entry * 10_000.0
    short_bps = (pos.short_avg_entry - short_q.ask) / pos.short_avg_entry * 10_000.0
    return long_bps + short_bps - long_fee_bps - short_fee_bps


def entry_signal_bps(
    spec: PairSpec,
    pos: Position,
    long_q: Quote,
    short_q: Quote,
) -> float:
    if pos.qty > 0:
        return float("-inf")

    if current_notional_usd(pos) >= spec.max_position_usd:
        return float("-inf")

    return executable_entry_bps(
        long_q,
        short_q,
        spec.long_leg.fee_bps_taker,
        spec.short_leg.fee_bps_taker,
    )


def exit_signal(
    spec: PairSpec,
    pos: Position,
    long_q: Quote,
    short_q: Quote,
    now_monotonic: float,
) -> tuple[str | None, float]:
    exit_edge = executable_exit_bps(
        pos,
        long_q,
        short_q,
        spec.long_leg.fee_bps_taker,
        spec.short_leg.fee_bps_taker,
    )
    if pos.qty <= 0:
        return None, exit_edge
    if exit_edge >= spec.exit_threshold_bps:
        return "target", exit_edge
    if exit_edge <= spec.stop_loss_bps:
        return "stop", exit_edge
    if pos.opened_at_monotonic > 0:
        held_for = now_monotonic - pos.opened_at_monotonic
        if held_for >= spec.max_hold_secs:
            return "time", exit_edge
    return None, exit_edge
