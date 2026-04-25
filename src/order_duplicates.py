"""Order duplicate detection and management.

Prevents placing duplicate orders and tracks open order history.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .client import PolymarketClient


@dataclass
class OpenOrder:
    """Represents an open order on the market."""
    order_id: str
    token_id: str
    side: str  # "BUY" or "SELL"
    price: float
    size: float
    remaining_size: Optional[float] = None  # Amount not yet filled
    created_at: Optional[str] = None


@dataclass
class OrderSnapshot:
    """Snapshot of orders at a point in time."""
    timestamp: str
    market_token_id: str
    market_slug: str
    outcome: str  # "YES" or "NO"
    orders: list[OpenOrder] = field(default_factory=list)


class OrderDuplicateDetector:
    """Detects and prevents duplicate orders from being placed."""

    def __init__(self, state_dir: str = "data"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.order_history_file = self.state_dir / "open_orders_history.jsonl"

    def detect_duplicates(
        self,
        client: PolymarketClient,
        market_slug: str,
        yes_token_id: str,
        no_token_id: str,
        quote_price_yes: float,
        quote_price_no: float,
        size: float,
        price_tolerance: float = 0.001,
    ) -> DuplicateCheckResult:
        """Check if we're about to place duplicate orders.

        Args:
            client: Polymarket client
            market_slug: Market identifier
            yes_token_id: YES outcome token ID
            no_token_id: NO outcome token ID
            quote_price_yes: Price we want to buy YES at
            quote_price_no: Price we want to buy NO at (complementary)
            size: Order size we want to place
            price_tolerance: How close prices need to be to be considered duplicates

        Returns:
            DuplicateCheckResult with findings
        """
        result = DuplicateCheckResult()

        try:
            open_orders = client.get_open_orders()
        except Exception as exc:
            result.error = str(exc)
            return result

        if not open_orders:
            return result

        # Normalize open orders
        orders_list = self._normalize_open_orders(open_orders)

        # Check for duplicates on each token
        yes_duplicates = self._find_duplicate_orders(
            orders_list, yes_token_id, "BUY", quote_price_yes, size, price_tolerance
        )
        no_duplicates = self._find_duplicate_orders(
            orders_list, no_token_id, "BUY", quote_price_no, size, price_tolerance
        )

        if yes_duplicates:
            result.has_duplicates = True
            result.duplicate_orders.extend(
                [
                    {
                        "token_id": yes_token_id,
                        "outcome": "YES",
                        "order_id": o.get("id"),
                        "side": o.get("side"),
                        "price": o.get("price"),
                        "size": o.get("size"),
                    }
                    for o in yes_duplicates
                ]
            )

        if no_duplicates:
            result.has_duplicates = True
            result.duplicate_orders.extend(
                [
                    {
                        "token_id": no_token_id,
                        "outcome": "NO",
                        "order_id": o.get("id"),
                        "side": o.get("side"),
                        "price": o.get("price"),
                        "size": o.get("size"),
                    }
                    for o in no_duplicates
                ]
            )

        # Log snapshot
        self._log_order_snapshot(market_slug, yes_token_id, "YES", orders_list, yes_duplicates)
        self._log_order_snapshot(market_slug, no_token_id, "NO", orders_list, no_duplicates)

        return result

    def _normalize_open_orders(self, response: Any) -> list[dict[str, Any]]:
        """Extract orders from various API response formats."""
        if isinstance(response, list):
            return response

        if isinstance(response, dict):
            # Try common response structures
            for key in ["orders", "data", "results", "open_orders"]:
                if key in response:
                    data = response[key]
                    if isinstance(data, list):
                        return data

        return []

    def _find_duplicate_orders(
        self,
        orders: list[dict[str, Any]],
        token_id: str,
        side: str,
        price: float,
        size: float,
        tolerance: float,
    ) -> list[dict[str, Any]]:
        """Find orders that match the given criteria."""
        duplicates = []

        for order in orders:
            order_token = order.get("token_id") or order.get("tokenId") or order.get("asset_id")
            order_side = (order.get("side") or "").upper()
            order_price = _to_float(order.get("price"))
            order_size = _to_float(order.get("size") or order.get("remainingSize"))

            if order_token != token_id or order_side != side.upper():
                continue

            if order_price is None or order_size is None:
                continue

            # Check if price and size are close enough to be considered a duplicate
            if abs(order_price - price) <= tolerance and abs(order_size - size) <= 0.1:
                duplicates.append(order)

        return duplicates

    def _log_order_snapshot(
        self,
        market_slug: str,
        token_id: str,
        outcome: str,
        all_orders: list[dict[str, Any]],
        duplicate_orders: list[dict[str, Any]],
    ) -> None:
        """Log order snapshot to history."""
        try:
            snapshot = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "market_slug": market_slug,
                "token_id": token_id,
                "outcome": outcome,
                "total_open_orders": len(all_orders),
                "duplicate_count": len(duplicate_orders),
                "duplicates": [
                    {
                        "order_id": o.get("id") or o.get("orderId"),
                        "side": o.get("side"),
                        "price": _to_float(o.get("price")),
                        "size": _to_float(o.get("size")),
                    }
                    for o in duplicate_orders
                ],
            }
            with self.order_history_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(snapshot) + "\n")
        except Exception as exc:
            print(f"Failed to log order snapshot: {exc}")

    def get_duplicate_history(self, market_slug: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve duplicate detection history."""
        if not self.order_history_file.exists():
            return []

        snapshots = []
        try:
            with self.order_history_file.open("r", encoding="utf-8") as f:
                for line in f:
                    try:
                        snapshot = json.loads(line)
                        if market_slug is None or snapshot.get("market_slug") == market_slug:
                            snapshots.append(snapshot)
                    except json.JSONDecodeError:
                        continue
        except Exception as exc:
            print(f"Failed to read order history: {exc}")

        return snapshots[-limit:]


@dataclass
class DuplicateCheckResult:
    """Result of duplicate order check."""
    has_duplicates: bool = False
    error: Optional[str] = None
    duplicate_orders: list[dict[str, Any]] = field(default_factory=list)

    def summary(self) -> str:
        """Human-readable summary."""
        if self.error:
            return f"Duplicate check error: {self.error}"
        if not self.has_duplicates:
            return "✓ No duplicates detected"
        count = len(self.duplicate_orders)
        return f"⚠ Found {count} duplicate order(s)"


def _to_float(value: Any) -> Optional[float]:
    """Convert value to float."""
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
