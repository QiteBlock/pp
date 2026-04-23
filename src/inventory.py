from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class MarketInventory:
    yes_shares: float = 0.0
    no_shares: float = 0.0
    realized_pnl: float = 0.0

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
            inventory.yes_shares += signed_size
            inventory.realized_pnl -= signed_size * price
        else:
            inventory.no_shares += signed_size
            inventory.realized_pnl -= signed_size * price
        return inventory
