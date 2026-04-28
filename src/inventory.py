from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

NEGATIVE_BALANCE_TOLERANCE = 1e-6


@dataclass
class MarketInventory:
    yes_shares: float = 0.0
    no_shares: float = 0.0
    realized_pnl: float = 0.0
    unwind_cycles_without_fill: int = 0
    last_fill_ts: float = 0.0

    @property
    def net_delta(self) -> float:
        return self.yes_shares - self.no_shares

    @property
    def gross_shares(self) -> float:
        return self.yes_shares + self.no_shares


@dataclass
class InventoryBook:
    markets: Dict[str, MarketInventory] = field(default_factory=dict)

    def get(self, market_key: str) -> MarketInventory:
        if market_key not in self.markets:
            self.markets[market_key] = MarketInventory()
        return self.markets[market_key]

    def apply_fill(self, market_key: str, outcome: str, side: str, size: float, price: float) -> MarketInventory:
        inventory = self.get(market_key)
        signed_size = size if side.upper() == "BUY" else -size
        if outcome.upper() == "YES":
            new_balance = inventory.yes_shares + signed_size
            if new_balance < -NEGATIVE_BALANCE_TOLERANCE:
                raise ValueError(
                    f"Fill would create negative YES balance for {market_key}: "
                    f"current={inventory.yes_shares:.6f} delta={signed_size:.6f}"
                )
            inventory.yes_shares = max(new_balance, 0.0)
            inventory.realized_pnl -= signed_size * price
        else:
            new_balance = inventory.no_shares + signed_size
            if new_balance < -NEGATIVE_BALANCE_TOLERANCE:
                raise ValueError(
                    f"Fill would create negative NO balance for {market_key}: "
                    f"current={inventory.no_shares:.6f} delta={signed_size:.6f}"
                )
            inventory.no_shares = max(new_balance, 0.0)
            inventory.realized_pnl -= signed_size * price
        inventory.unwind_cycles_without_fill = 0
        return inventory
