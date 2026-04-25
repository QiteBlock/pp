"""Fill tracking and state persistence for the market maker bot.

Handles:
- Polling for fills from the Polymarket API
- Persisting last fetch timestamp to avoid reprocessing
- Applying fills to the inventory book
- Logging fills to analytics
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from .analytics import AnalyticsWriter, FillRecord
from .client import PolymarketClient
from .inventory import InventoryBook, MarketInventory


@dataclass
class FillState:
    """Tracks the state of fill polling."""
    last_fetch_ts: int = 0  # milliseconds since epoch when we last successfully fetched fills
    last_processed_fill_id: Optional[str] = None  # ID of the last fill we processed


class FillPoller:
    """Manages periodic fetching and processing of fills."""

    def __init__(
        self,
        client: PolymarketClient,
        inventory: InventoryBook,
        analytics: AnalyticsWriter,
        state_dir: str = "data",
        fill_handler: Optional[Callable[[FillRecord, MarketInventory], None]] = None,
    ):
        self.client = client
        self.inventory = inventory
        self.analytics = analytics
        self.fill_handler = fill_handler
        self.state_path = Path(state_dir) / "fill_state.json"
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> FillState:
        """Load fill state from disk."""
        if not self.state_path.exists():
            return FillState()
        try:
            with self.state_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            return FillState(**data)
        except Exception as exc:
            print(f"Failed to load fill state: {exc}, starting fresh")
            return FillState()

    def _save_state(self) -> None:
        """Persist fill state to disk."""
        try:
            with self.state_path.open("w", encoding="utf-8") as handle:
                json.dump(asdict(self.state), handle)
        except Exception as exc:
            print(f"Failed to save fill state: {exc}")

    def poll_fills(self, market_slug_mapping: dict[str, tuple[str, str]]) -> int:
        """Poll for new fills and apply them to inventory.

        Args:
            market_slug_mapping: Maps token_id -> (market_slug, outcome)
                where outcome is "YES" or "NO"

        Returns:
            Number of new fills processed
        """
        try:
            fills = self.client.get_user_fills(since_ts=self.state.last_fetch_ts if self.state.last_fetch_ts > 0 else None)
        except Exception as exc:
            self.analytics.log_event("fill_poll_error", {"error": str(exc)})
            return 0

        if not fills:
            return 0

        processed_count = 0
        current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for fill in fills:
            if self._should_skip_fill(fill):
                continue

            if not self._apply_fill(fill, market_slug_mapping):
                continue

            processed_count += 1
            self.state.last_processed_fill_id = fill.get("id") or fill.get("orderId")

        # Update last fetch timestamp
        if fills:
            last_fill_ts = self._extract_timestamp(fills[-1])
            if last_fill_ts:
                self.state.last_fetch_ts = last_fill_ts

        self.state.last_fetch_ts = max(self.state.last_fetch_ts, current_time_ms)
        if processed_count > 0:
            self._save_state()

        return processed_count

    def _should_skip_fill(self, fill: dict[str, Any]) -> bool:
        """Check if we should skip processing this fill (already processed)."""
        fill_id = fill.get("id") or fill.get("orderId")
        if fill_id and self.state.last_processed_fill_id == fill_id:
            return True
        return False

    def _extract_timestamp(self, fill: dict[str, Any]) -> Optional[int]:
        """Extract timestamp from fill record in milliseconds."""
        for key in ["createdAt", "timestamp", "created_at", "time"]:
            ts = fill.get(key)
            if ts:
                try:
                    if isinstance(ts, str):
                        # Parse ISO format timestamp
                        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        return int(dt.timestamp() * 1000)
                    elif isinstance(ts, (int, float)):
                        # Assume milliseconds if large, seconds if small
                        if ts > 100000000000:  # definitely milliseconds
                            return int(ts)
                        else:  # likely seconds
                            return int(ts * 1000)
                except (ValueError, TypeError):
                    continue
        return None

    def _apply_fill(self, fill: dict[str, Any], market_slug_mapping: dict[str, tuple[str, str]]) -> bool:
        """Apply a fill to inventory and log it.

        Returns True if successfully applied, False otherwise.
        """
        token_id = fill.get("token_id") or fill.get("tokenId") or fill.get("asset_id")
        if not token_id or token_id not in market_slug_mapping:
            return False

        side = (fill.get("side") or fill.get("Side") or "").upper()
        if side not in ("BUY", "SELL"):
            return False

        size = _to_float(fill.get("size") or fill.get("Size") or fill.get("amount"))
        price = _to_float(fill.get("price") or fill.get("Price"))
        if size is None or price is None or size <= 0:
            return False

        market_slug, outcome = market_slug_mapping[token_id]
        timestamp = self._extract_timestamp(fill)
        ts_str = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).isoformat() if timestamp else datetime.now(timezone.utc).isoformat()

        try:
            # Apply to inventory
            inventory = self.inventory.apply_fill(market_slug, outcome, side, size, price)

            # Log fill
            record = FillRecord(
                timestamp=ts_str,
                market=market_slug,
                outcome=outcome,
                side=side,
                size=size,
                price=price,
            )
            self.analytics.log_fill(record)
            self.analytics.log_event(
                "fill_applied",
                {
                    "market": market_slug,
                    "token_id": token_id,
                    "outcome": outcome,
                    "side": side,
                    "size": size,
                    "price": price,
                },
            )
            if self.fill_handler is not None:
                try:
                    self.fill_handler(record, inventory)
                except Exception as exc:
                    self.analytics.log_event("fill_handler_error", {"market": market_slug, "error": str(exc)})
            return True
        except Exception as exc:
            print(f"Error applying fill for {market_slug}: {exc}")
            self.analytics.log_event("fill_apply_error", {"market": market_slug, "error": str(exc)})
            return False


def _to_float(value: Any) -> Optional[float]:
    """Convert value to float, handling None and invalid inputs."""
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
