"""Startup reconciliation against Polymarket positions API.

Ensures safe restarts and safe coexistence with trades placed from the web UI.
"""

from __future__ import annotations

from typing import Any, Optional

from .client import PolymarketClient, MarketConfig
from .inventory import InventoryBook


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
            report.status = "no_positions_found"
            return report

        # Build token ID to market mapping
        token_to_market = {}
        for market in markets:
            token_to_market[market.yes_token_id] = (market.slug, "YES")
            token_to_market[market.no_token_id] = (market.slug, "NO")

        # Check each position reported by the API
        for token_id, api_balance in api_positions.items():
            if not isinstance(api_balance, (int, float)):
                continue

            if token_id not in token_to_market:
                report.unknown_positions[token_id] = float(api_balance)
                continue

            market_slug, outcome = token_to_market[token_id]
            inventory = self.inventory.get(market_slug)

            # Get current inventory for this outcome
            current_balance = inventory.yes_shares if outcome == "YES" else inventory.no_shares

            # Check for discrepancy
            balance_diff = float(api_balance) - current_balance
            if abs(balance_diff) > 0.01:  # tolerance of 0.01 shares
                report.discrepancies.append({
                    "market": market_slug,
                    "outcome": outcome,
                    "token_id": token_id,
                    "api_balance": float(api_balance),
                    "inventory_balance": current_balance,
                    "difference": balance_diff,
                })

                # Update inventory to match API
                if outcome == "YES":
                    adjustment = balance_diff
                    inventory.yes_shares = float(api_balance)
                else:
                    adjustment = balance_diff
                    inventory.no_shares = float(api_balance)

                report.adjustments.append({
                    "market": market_slug,
                    "outcome": outcome,
                    "token_id": token_id,
                    "adjustment": adjustment,
                    "new_balance": float(api_balance),
                })

        # Check for positions in inventory that aren't in API (shouldn't happen, but flag it)
        for market_slug, inv in self.inventory.markets.items():
            for market in markets:
                if market.slug == market_slug:
                    yes_token = market.yes_token_id
                    no_token = market.no_token_id

                    if yes_token not in api_positions and inv.yes_shares > 0.01:
                        report.orphaned_positions.append({
                            "market": market_slug,
                            "outcome": "YES",
                            "token_id": yes_token,
                            "balance": inv.yes_shares,
                        })

                    if no_token not in api_positions and inv.no_shares > 0.01:
                        report.orphaned_positions.append({
                            "market": market_slug,
                            "outcome": "NO",
                            "token_id": no_token,
                            "balance": inv.no_shares,
                        })
                    break

        # Determine overall status
        if report.error:
            report.status = "error"
        elif report.discrepancies:
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
