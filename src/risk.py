from __future__ import annotations

from dataclasses import dataclass

from .inventory import InventoryBook, MarketInventory


@dataclass
class RiskConfig:
    max_yes_shares_per_market: float
    max_no_shares_per_market: float
    max_net_delta_per_market: float
    max_total_exposure_usdc: float
    max_drawdown_usdc: float
    min_days_to_resolution: float
    min_order_size: float


def compute_market_exposure(inventory: MarketInventory, mark_price: float) -> float:
    yes_exposure = inventory.yes_shares * mark_price
    no_exposure = inventory.no_shares * (1.0 - mark_price)
    return max(yes_exposure, 0.0) + max(no_exposure, 0.0)


def total_exposure(book: InventoryBook, mark_prices: dict[str, float]) -> float:
    total = 0.0
    for market_key, inventory in book.markets.items():
        total += compute_market_exposure(inventory, mark_prices.get(market_key, 0.5))
    return total


def can_quote_market(config: RiskConfig, inventory: MarketInventory, fair_value: float, days_to_resolution: float) -> tuple[bool, str]:
    if days_to_resolution < config.min_days_to_resolution:
        return False, "market is too close to resolution"
    if inventory.yes_shares > config.max_yes_shares_per_market:
        return False, "yes inventory limit breached"
    if inventory.no_shares > config.max_no_shares_per_market:
        return False, "no inventory limit breached"
    if abs(inventory.net_delta) > config.max_net_delta_per_market:
        return False, "delta neutrality limit breached"
    if inventory.realized_pnl < -config.max_drawdown_usdc:
        return False, "drawdown limit breached"
    return True, ""
