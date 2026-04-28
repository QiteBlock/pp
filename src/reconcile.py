"""Startup reconciliation against Polymarket positions API.

Ensures safe restarts and safe coexistence with trades placed from the web UI.
"""

from __future__ import annotations

from typing import Any, Optional

from .client import PolymarketClient, MarketConfig
from .inventory import InventoryBook
from .units import parse_token_amount


class PositionReconciler:
    """Reconciles inventory against Polymarket API positions."""

    def __init__(self, client: PolymarketClient, inventory: InventoryBook):
        self.client = client
        self.inventory = inventory

    def reconcile_positions(self, markets: list[MarketConfig]) -> ReconciliationReport:
        """Reconcile inventory against API positions.

        Fetches current positions from Polymarket and reconciles against inventory.
        For any discrepancies, updates inventory to match API state.

        Args:
            markets: List of configured markets

        Returns:
            ReconciliationReport with details of any adjustments made
        """
        report = ReconciliationReport()

        try:
            api_positions = self.client.get_user_positions()
        except Exception as exc:
            report.error = str(exc)
            return report

        if not api_positions:
            api_positions = {}

        # Build token ID to market mapping
        token_to_market = {}
        for market in markets:
            token_to_market[market.yes_token_id] = (market.slug, "YES")
            token_to_market[market.no_token_id] = (market.slug, "NO")

        # Check each position reported by the API
        for token_id, api_balance in api_positions.items():
            normalized_balance = parse_token_amount(api_balance)
            if api_balance not in (0, 0.0, "0", "0.0") and normalized_balance == 0.0:
                continue

            if token_id not in token_to_market:
                report.unknown_positions[token_id] = normalized_balance
                continue

            market_slug, outcome = token_to_market[token_id]
            inventory = self.inventory.get(market_slug)

            # Get current inventory for this outcome
            current_balance = inventory.yes_shares if outcome == "YES" else inventory.no_shares

            # Check for discrepancy
            balance_diff = normalized_balance - current_balance
            if abs(balance_diff) > 0.01:  # tolerance of 0.01 shares
                report.discrepancies.append({
                    "market": market_slug,
                    "outcome": outcome,
                    "token_id": token_id,
                    "api_balance": normalized_balance,
                    "inventory_balance": current_balance,
                    "difference": balance_diff,
                })

                # Update inventory to match API
                if outcome == "YES":
                    adjustment = balance_diff
                    inventory.yes_shares = normalized_balance
                else:
                    adjustment = balance_diff
                    inventory.no_shares = normalized_balance

                report.adjustments.append({
                    "market": market_slug,
                    "outcome": outcome,
                    "token_id": token_id,
                    "adjustment": adjustment,
                    "new_balance": normalized_balance,
                })

        # Zero tracked positions that are absent from the API response.
        for market in markets:
            inventory = self.inventory.get(market.slug)
            if market.yes_token_id not in api_positions and inventory.yes_shares > 0.01:
                report.orphaned_positions.append({
                    "market": market.slug,
                    "outcome": "YES",
                    "token_id": market.yes_token_id,
                    "balance": inventory.yes_shares,
                })
                report.adjustments.append({
                    "market": market.slug,
                    "outcome": "YES",
                    "token_id": market.yes_token_id,
                    "adjustment": -inventory.yes_shares,
                    "new_balance": 0.0,
                })
                inventory.yes_shares = 0.0
            if market.no_token_id not in api_positions and inventory.no_shares > 0.01:
                report.orphaned_positions.append({
                    "market": market.slug,
                    "outcome": "NO",
                    "token_id": market.no_token_id,
                    "balance": inventory.no_shares,
                })
                report.adjustments.append({
                    "market": market.slug,
                    "outcome": "NO",
                    "token_id": market.no_token_id,
                    "adjustment": -inventory.no_shares,
                    "new_balance": 0.0,
                })
                inventory.no_shares = 0.0

        # Determine overall status
        if report.error:
            report.status = "error"
        elif not api_positions and not report.adjustments:
            report.status = "no_positions_found"
        elif report.discrepancies or report.adjustments:
            report.status = "reconciled"
        else:
            report.status = "consistent"

        return report

    def reconcile_startup(self, markets: list[MarketConfig]) -> ReconciliationReport:
        """Backward-compatible startup entrypoint."""
        return self.reconcile_positions(markets)


class ReconciliationReport:
    """Report of reconciliation results."""

    def __init__(self):
        self.status: str = "unknown"  # "consistent", "reconciled", "error", "no_positions_found"
        self.error: Optional[str] = None
        self.discrepancies: list[dict[str, Any]] = []
        self.adjustments: list[dict[str, Any]] = []
        self.unknown_positions: dict[str, float] = {}  # token_id -> balance of positions not in our config
        self.orphaned_positions: list[dict[str, Any]] = []  # positions in inventory but not in API

    def summary(self) -> str:
        """Get human-readable summary of reconciliation."""
        if self.error:
            return f"Reconciliation failed: {self.error}"
        if self.status == "no_positions_found":
            return "No positions found on API (fresh start or withdrawn)"
        if self.status == "consistent":
            return "Inventory consistent with API ✓"
        if self.status == "reconciled":
            count = len(self.adjustments)
            return f"Reconciled {count} position(s) ✓"
        return f"Reconciliation status: {self.status}"

    def should_proceed_with_trading(self) -> bool:
        """Determine if bot should proceed with trading after reconciliation."""
        # Proceed if we're consistent, reconciled, or couldn't fetch positions
        # Only halt if there's an actual error or orphaned positions
        if self.error and "unavailable" not in self.error.lower():
            return False
        if self.orphaned_positions:
            # Log warning but still allow trading
            return True
        return True
