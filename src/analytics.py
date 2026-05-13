from __future__ import annotations

import csv
import json
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


@dataclass
class FillRecord:
    timestamp: str
    market: str
    outcome: str
    side: str
    size: float
    price: float
    fair_value: float | None = None
    spread_capture: float | None = None


class AnalyticsWriter:
    def __init__(self, log_dir: str) -> None:
        self.base_path = Path(log_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.fills_csv = self.base_path / "fills.csv"
        self.events_jsonl = self.base_path / "events.jsonl"
        self._ensure_fill_header()

        self.order_states: dict[str, dict[str, Any]] = {}
        self.open_lots: dict[str, dict[str, deque[dict[str, Any]]]] = defaultdict(lambda: defaultdict(deque))
        self.pending_fill_followups: dict[str, dict[str, Any]] = {}
        self.skip_reason_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.cancel_timestamps: dict[str, deque[float]] = defaultdict(deque)
        self.fill_timestamps: dict[str, deque[float]] = defaultdict(deque)
        self.reward_fill_records: list[dict[str, Any]] = []
        self.market_entered_at: dict[str, float] = {}
        self._last_skip_summary_ts = 0.0
        self._last_churn_summary_ts = 0.0
        self._last_reward_snapshot_ts = 0.0
        self._last_state_gc_ts = 0.0

    def _ensure_fill_header(self) -> None:
        if self.fills_csv.exists():
            return
        with self.fills_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(FillRecord.__annotations__.keys()))
            writer.writeheader()

    def log_fill(self, record: FillRecord) -> None:
        with self.fills_csv.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(FillRecord.__annotations__.keys()))
            writer.writerow(asdict(record))

    def log_event(self, event_type: str, payload: dict[str, Any]) -> None:
        message = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "payload": payload,
        }
        with self.events_jsonl.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(message) + "\n")

    def note_market_active(self, market: str) -> None:
        self.market_entered_at.setdefault(market, time.time())

    def note_skip_reason(self, market: str, reason: str) -> None:
        if not market or not reason:
            return
        self.skip_reason_counts[market][reason] += 1

    def note_cancel(self, market: str) -> None:
        if market:
            self.cancel_timestamps[market].append(time.time())

    def note_fill(self, market: str) -> None:
        if market:
            self.fill_timestamps[market].append(time.time())

    def record_order_placement(
        self,
        order_id: str,
        market: str,
        side: str,
        outcome: str,
        price: float,
        size: float,
        reward_eligible: bool,
        reward_rate_per_day: Optional[float],
        fair_value: Optional[float],
        rewards_max_spread: Optional[float] = None,
        placement_mark: Optional[float] = None,
    ) -> None:
        if not order_id:
            return
        self.order_states[order_id] = {
            "order_id": order_id,
            "market": market,
            "side": side,
            "outcome": outcome,
            "price": price,
            "size": size,
            "reward_eligible": reward_eligible,
            "reward_rate_per_day": reward_rate_per_day,
            "fair_value": fair_value,
            "rewards_max_spread": rewards_max_spread,
            "placement_mark": placement_mark,
            "place_ts": time.time(),
            "fill_ts": None,
            "cancel_ts": None,
        }

    def record_order_cancel(
        self,
        order_id: str,
        market: str,
        side: Optional[str] = None,
        outcome: Optional[str] = None,
        price: Optional[float] = None,
        size: Optional[float] = None,
    ) -> None:
        if not order_id:
            return
        state = self.order_states.get(order_id, {}).copy()
        if not state:
            state = {
                "order_id": order_id,
                "market": market,
                "side": side,
                "outcome": outcome,
                "price": price,
                "size": size,
                "place_ts": None,
            }
        if state.get("cancel_ts"):
            cancel_ts = float(state["cancel_ts"])
        else:
            cancel_ts = time.time()
            state["cancel_ts"] = cancel_ts
        time_on_book_ms = None
        if state.get("place_ts"):
            time_on_book_ms = max(int((cancel_ts - float(state["place_ts"])) * 1000), 0)
        reward_estimate = self._build_reward_estimate_record(
            state=state,
            event_ts=cancel_ts,
            executed_size=_to_optional_float(state.get("size")),
            event_type="cancel",
        )
        if reward_estimate is not None:
            self.reward_fill_records.append(reward_estimate)
            self.log_event("reward_estimate_on_cancel", reward_estimate)
        self.log_event(
            "quote_lifecycle",
            {
                "order_id": order_id,
                "market": state.get("market") or market,
                "side": state.get("side") or side,
                "outcome": state.get("outcome") or outcome,
                "price": state.get("price") if state.get("price") is not None else price,
                "size": state.get("size") if state.get("size") is not None else size,
                "place_ts": _iso_from_epoch(state.get("place_ts")),
                "cancel_ts": _iso_from_epoch(cancel_ts),
                "fill_ts": _iso_from_epoch(state.get("fill_ts")),
                "time_on_book_ms": time_on_book_ms,
                "fate": "canceled",
            },
        )

    def record_fill_observability(
        self,
        fill_id: str,
        market: str,
        token_id: str,
        outcome: str,
        side: str,
        size: float,
        price: float,
        fill_ts: float,
        order_id: Optional[str],
    ) -> None:
        self.note_market_active(market)
        self.note_fill(market)
        if fill_id and fill_id not in self.pending_fill_followups:
            self.pending_fill_followups[fill_id] = {
                "fill_id": fill_id,
                "market": market,
                "token_id": token_id,
                "outcome": outcome,
                "side": side,
                "size": size,
                "fill_price": price,
                "mark_at_fill": self._mark_at_fill(order_id, price),
                "fill_ts": fill_ts,
                "checkpoint_30_ts": fill_ts + 30.0,
                "checkpoint_300_ts": fill_ts + 300.0,
                "mark_at_30s": None,
                "mark_at_5min": None,
            }

        if order_id:
            state = self.order_states.get(order_id)
            if state:
                state["fill_ts"] = fill_ts
                time_on_book_ms = None
                if state.get("place_ts"):
                    time_on_book_ms = max(int((fill_ts - float(state["place_ts"])) * 1000), 0)
                self.log_event(
                    "quote_lifecycle",
                    {
                        "order_id": order_id,
                        "market": state.get("market") or market,
                        "side": state.get("side") or side,
                        "outcome": state.get("outcome") or outcome,
                        "price": state.get("price"),
                        "size": state.get("size"),
                        "place_ts": _iso_from_epoch(state.get("place_ts")),
                        "cancel_ts": _iso_from_epoch(state.get("cancel_ts")),
                        "fill_ts": _iso_from_epoch(fill_ts),
                        "time_on_book_ms": time_on_book_ms,
                        "fate": "filled",
                    },
                )
                if state.get("reward_eligible"):
                    reward_estimate = self._build_reward_estimate_record(
                        state=state,
                        event_ts=fill_ts,
                        executed_size=size,
                        event_type="fill",
                    )
                    if reward_estimate is not None:
                        self.reward_fill_records.append(reward_estimate)
                        self.log_event("reward_eligible_fill", reward_estimate)

        self._record_round_trip(market, outcome, side, size, price, fill_ts)

    def emit_quote_cycle_flow(
        self,
        market: str,
        recent_buy_count: int,
        recent_sell_count: int,
        buy_ratio: Optional[float],
        mid_move_last_cycle: float,
        would_skip_yes: bool,
        would_skip_no: bool,
    ) -> None:
        self.log_event(
            "quote_cycle_flow",
            {
                "market": market,
                "recent_buy_count": recent_buy_count,
                "recent_sell_count": recent_sell_count,
                "buy_ratio": buy_ratio,
                "mid_move_last_cycle": mid_move_last_cycle,
                "would_skip_yes": would_skip_yes,
                "would_skip_no": would_skip_no,
            },
        )

    def emit_market_pnl_snapshot(
        self,
        market: str,
        inventory: Any,
        current_mark: Optional[float],
    ) -> None:
        self.note_market_active(market)
        mark = _to_optional_float(current_mark) or _to_optional_float(getattr(inventory, "last_mark_price", 0.0)) or 0.5
        unrealized = self._market_unrealized_pnl(market, mark)
        rewards_earned = sum(
            float(item.get("est_reward_per_fill") or 0.0)
            for item in self.reward_fill_records
            if item.get("market") == market
        )
        fills_count = len([ts for ts in self.fill_timestamps.get(market, [])])
        entered_at = self.market_entered_at.get(market, time.time())
        time_in_market_min = max((time.time() - entered_at) / 60.0, 0.0)
        self.log_event(
            "market_pnl_snapshot",
            {
                "market": market,
                "realized_pnl": inventory.realized_pnl,
                "unrealized_mark_pnl": unrealized,
                "rewards_earned": rewards_earned,
                "net_pnl": inventory.realized_pnl + unrealized + rewards_earned,
                "gross_shares": inventory.gross_shares,
                "fills_count": fills_count,
                "time_in_market_min": time_in_market_min,
            },
        )

    def emit_merge_pnl_impact(
        self,
        market: str,
        paired_shares: float,
        usdc_received: float,
        gas_used: Optional[float],
    ) -> None:
        avg_yes_basis, avg_no_basis = self.consume_merge_lots(market, paired_shares)
        spread_capture = None
        if avg_yes_basis is not None and avg_no_basis is not None:
            spread_capture = usdc_received - paired_shares * (avg_yes_basis + avg_no_basis)
        self.log_event(
            "merge_pnl_impact",
            {
                "market": market,
                "paired_shares": paired_shares,
                "avg_yes_basis": avg_yes_basis,
                "avg_no_basis": avg_no_basis,
                "usdc_received": usdc_received,
                "spread_captured_usdc": spread_capture,
                "gas_used": gas_used,
                "net_pnl_delta": spread_capture,
            },
        )

    def process_pending_followups(self, client: Any) -> None:
        now = time.time()
        completed: list[str] = []
        for fill_id, entry in self.pending_fill_followups.items():
            token_id = entry.get("token_id")
            if not token_id:
                completed.append(fill_id)
                continue
            if entry.get("mark_at_30s") is None and now >= float(entry["checkpoint_30_ts"]):
                mark = self._fetch_mark(client, token_id)
                if mark is not None:
                    entry["mark_at_30s"] = mark
            if entry.get("mark_at_5min") is None and now >= float(entry["checkpoint_300_ts"]):
                mark = self._fetch_mark(client, token_id)
                if mark is not None:
                    entry["mark_at_5min"] = mark
            if entry.get("mark_at_30s") is not None and entry.get("mark_at_5min") is not None:
                self.log_event(
                    "fill_mark_followup",
                    {
                        "fill_id": fill_id,
                        "market": entry["market"],
                        "side": entry["side"],
                        "fill_price": entry["fill_price"],
                        "mark_at_fill": entry["mark_at_fill"],
                        "mark_at_30s": entry["mark_at_30s"],
                        "mark_at_5min": entry["mark_at_5min"],
                        "mtm_pnl_30s": self._mark_pnl(entry, entry["mark_at_30s"]),
                        "mtm_pnl_5min": self._mark_pnl(entry, entry["mark_at_5min"]),
                    },
                )
                completed.append(fill_id)
        for fill_id in completed:
            self.pending_fill_followups.pop(fill_id, None)

    def emit_periodic_summaries(self) -> None:
        now = time.time()
        if now - self._last_state_gc_ts >= 300.0:
            self._gc_order_states(now)
            self._last_state_gc_ts = now
        if now - self._last_skip_summary_ts >= 300:
            for market, reasons in list(self.skip_reason_counts.items()):
                if reasons:
                    self.log_event("quote_skipped_summary", {"market": market, "reasons": dict(reasons)})
            self.skip_reason_counts.clear()
            self._last_skip_summary_ts = now

        if now - self._last_churn_summary_ts >= 300:
            for market in set(self.cancel_timestamps) | set(self.fill_timestamps):
                cancels = self._recent_count(self.cancel_timestamps[market], now, 300)
                fills = self._recent_count(self.fill_timestamps[market], now, 300)
                fills_per_cancel = (fills / cancels) if cancels > 0 else None
                self.log_event(
                    "cancel_replace_rate",
                    {
                        "market": market,
                        "cancels_per_minute": cancels / 5.0,
                        "fills_per_cancel": fills_per_cancel,
                    },
                )
            self._last_churn_summary_ts = now

        if now - self._last_reward_snapshot_ts >= 3600:
            cutoff = now - 86400
            records = [item for item in self.reward_fill_records if float(item.get("ts") or 0.0) >= cutoff]
            rewards_earned_24h = sum(float(item.get("est_reward_per_fill") or 0.0) for item in records)
            fill_records = [item for item in records if item.get("event_type") == "fill"]
            fills_count = len(fill_records)
            avg_reward = (
                sum(float(item.get("est_reward_per_fill") or 0.0) for item in fill_records) / fills_count
            ) if fills_count else None
            self.log_event(
                "reward_pnl_snapshot",
                {
                    "rewards_earned_24h": rewards_earned_24h,
                    "fills_count": fills_count,
                    "avg_reward_per_fill": avg_reward,
                    "cancel_estimate_count": len(records) - fills_count,
                },
            )
            self._last_reward_snapshot_ts = now

    def _build_reward_estimate_record(
        self,
        state: dict[str, Any],
        event_ts: float,
        executed_size: Optional[float],
        event_type: str,
    ) -> Optional[dict[str, Any]]:
        if not state.get("reward_eligible") or not state.get("place_ts"):
            return None
        reward_rate = _to_optional_float(state.get("reward_rate_per_day"))
        size = _to_optional_float(executed_size)
        if reward_rate is None or size is None or size <= 0:
            return None
        fair_value = _to_optional_float(state.get("fair_value"))
        price = _to_optional_float(state.get("price"))
        rewards_max_spread = _to_optional_float(state.get("rewards_max_spread"))
        mid_dist_to_fair = abs(price - fair_value) if price is not None and fair_value is not None else None
        quality = 1.0
        if rewards_max_spread and rewards_max_spread > 0 and mid_dist_to_fair is not None:
            quality = max(0.0, (rewards_max_spread - mid_dist_to_fair) / rewards_max_spread)
        time_on_book_sec = max(event_ts - float(state.get("place_ts") or event_ts), 0.0)
        est_reward_per_fill = reward_rate * (time_on_book_sec / 86400.0) * (size / 100.0) * quality
        return {
            "ts": event_ts,
            "event_type": event_type,
            "market": state.get("market"),
            "size": size,
            "reward_rate_per_day": reward_rate,
            "mid_dist_to_fair": mid_dist_to_fair,
            "rewards_max_spread": rewards_max_spread,
            "quality": quality,
            "time_on_book_sec": time_on_book_sec,
            "est_reward_per_fill": est_reward_per_fill,
        }

    def estimate_avg_basis(self, market: str, outcome: str, shares: float) -> Optional[float]:
        return self._fifo_average_price(market, outcome, shares, mutate=False)

    def consume_merge_lots(self, market: str, shares: float) -> tuple[Optional[float], Optional[float]]:
        return (
            self._fifo_average_price(market, "YES", shares, mutate=True),
            self._fifo_average_price(market, "NO", shares, mutate=True),
        )

    def _fifo_average_price(self, market: str, outcome: str, shares: float, mutate: bool) -> Optional[float]:
        lots = self.open_lots.get(market, {}).get(outcome.upper(), deque())
        remaining = max(float(shares), 0.0)
        if remaining <= 0 or not lots:
            return None
        total_cost = 0.0
        total_size = 0.0
        if mutate:
            iterable = lots
        else:
            iterable = deque({"size": float(lot["size"]), "price": float(lot["price"]), "ts": lot["ts"]} for lot in lots)
        while iterable and remaining > 0:
            lot = iterable[0]
            if remaining <= 0:
                break
            use_size = min(float(lot["size"]), remaining)
            total_cost += use_size * float(lot["price"])
            total_size += use_size
            remaining -= use_size
            if mutate:
                lot["size"] = float(lot["size"]) - use_size
                if lot["size"] <= 1e-9:
                    iterable.popleft()
        if total_size <= 0:
            return None
        return total_cost / total_size

    def _market_unrealized_pnl(self, market: str, yes_mark: float) -> float:
        unrealized = 0.0
        for lot in self.open_lots.get(market, {}).get("YES", deque()):
            unrealized += float(lot["size"]) * (yes_mark - float(lot["price"]))
        no_mark = 1.0 - yes_mark
        for lot in self.open_lots.get(market, {}).get("NO", deque()):
            unrealized += float(lot["size"]) * (no_mark - float(lot["price"]))
        return unrealized

    def _record_round_trip(self, market: str, outcome: str, side: str, size: float, price: float, fill_ts: float) -> None:
        lots = self.open_lots[market][outcome.upper()]
        if side.upper() == "BUY":
            lots.append({"size": size, "price": price, "ts": fill_ts})
            return
        remaining = size
        while remaining > 1e-9 and lots:
            buy_lot = lots[0]
            matched = min(float(buy_lot["size"]), remaining)
            gross_pnl = matched * (price - float(buy_lot["price"]))
            gross_bps = ((price - float(buy_lot["price"])) / float(buy_lot["price"]) * 10000.0) if float(buy_lot["price"]) > 0 else None
            self.log_event(
                "round_trip_pnl",
                {
                    "market": market,
                    "outcome": outcome.upper(),
                    "buy_price": float(buy_lot["price"]),
                    "buy_size": matched,
                    "buy_ts": _iso_from_epoch(float(buy_lot["ts"])),
                    "sell_price": price,
                    "sell_size": matched,
                    "sell_ts": _iso_from_epoch(fill_ts),
                    "gross_pnl": gross_pnl,
                    "gross_bps": gross_bps,
                    "is_lottery": bool(gross_bps is not None and abs(gross_bps) > 5000.0),
                },
            )
            buy_lot["size"] = float(buy_lot["size"]) - matched
            remaining -= matched
            if buy_lot["size"] <= 1e-9:
                lots.popleft()

    def _fetch_mark(self, client: Any, token_id: str) -> Optional[float]:
        try:
            book = client.get_orderbook(token_id)
        except Exception:
            return None
        return _to_optional_float(book.midpoint) or _to_optional_float(book.last_trade_price)

    def _mark_at_fill(self, order_id: Optional[str], fill_price: float) -> float:
        if order_id:
            state = self.order_states.get(order_id)
            if state and _to_optional_float(state.get("placement_mark")) is not None:
                return float(state["placement_mark"])
            if state and _to_optional_float(state.get("fair_value")) is not None:
                return float(state["fair_value"])
            if state and _to_optional_float(state.get("price")) is not None:
                return float(state["price"])
        return fill_price

    def _mark_pnl(self, entry: dict[str, Any], mark: float) -> float:
        fill_price = float(entry["fill_price"])
        size = float(entry["size"])
        if str(entry["side"]).upper() == "BUY":
            return (mark - fill_price) * size
        return (fill_price - mark) * size

    def _recent_count(self, timestamps: deque[float], now: float, window_seconds: float) -> int:
        cutoff = now - window_seconds
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()
        return len(timestamps)

    def _gc_order_states(self, now: float) -> None:
        cutoff = now - 3600.0
        stale_ids = [
            order_id
            for order_id, state in self.order_states.items()
            if self._state_reference_ts(state) <= cutoff
        ]
        for order_id in stale_ids:
            self.order_states.pop(order_id, None)
        if stale_ids:
            self.log_event(
                "order_states_gc",
                {
                    "removed": len(stale_ids),
                    "remaining": len(self.order_states),
                    "cutoff_ts": cutoff,
                },
            )

    def _state_reference_ts(self, state: dict[str, Any]) -> float:
        timestamps = [
            _to_optional_float(state.get("place_ts")) or 0.0,
            _to_optional_float(state.get("fill_ts")) or 0.0,
            _to_optional_float(state.get("cancel_ts")) or 0.0,
        ]
        return max(timestamps)


def _iso_from_epoch(value: Any) -> Optional[str]:
    if value in (None, "", 0):
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _to_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
