#!/usr/bin/env python
"""Simple integration test for fill tracking and reconciliation."""

from src.inventory import InventoryBook, MarketInventory
from src.fills import FillPoller, FillState, _to_float
from src.reconcile import PositionReconciler, ReconciliationReport

def test_inventory_fill():
    """Test that fills are correctly applied to inventory."""
    inventory = InventoryBook()
    
    # Buy 100 YES at 0.50
    inventory.apply_fill("test-market", "YES", "BUY", 100.0, 0.50)
    assert inventory.get("test-market").yes_shares == 100.0
    assert inventory.get("test-market").realized_pnl == -50.0  # cost basis
    
    # Sell 50 YES at 0.60
    inventory.apply_fill("test-market", "YES", "SELL", 50.0, 0.60)
    assert inventory.get("test-market").yes_shares == 50.0
    assert inventory.get("test-market").realized_pnl == -50.0 + 30.0  # cost + profit
    
    # Buy 100 NO at 0.40
    inventory.apply_fill("test-market", "NO", "BUY", 100.0, 0.40)
    assert inventory.get("test-market").no_shares == 100.0
    assert inventory.get("test-market").realized_pnl == -20.0 + (-40.0)
    
    print("✓ Inventory fill test passed")

def test_fill_state():
    """Test fill state serialization."""
    state = FillState(last_fetch_ts=1234567890, last_processed_fill_id="fill_123")
    
    from dataclasses import asdict
    state_dict = asdict(state)
    assert state_dict["last_fetch_ts"] == 1234567890
    assert state_dict["last_processed_fill_id"] == "fill_123"
    
    print("✓ Fill state test passed")

def test_to_float():
    """Test float conversion utility."""
    assert _to_float(123.45) == 123.45
    assert _to_float("123.45") == 123.45
    assert _to_float(None) is None
    assert _to_float("") is None
    assert _to_float("invalid") is None
    
    print("✓ Float conversion test passed")

def test_reconciliation_report():
    """Test reconciliation report."""
    report = ReconciliationReport()
    report.status = "consistent"
    
    assert report.summary() == "Inventory consistent with API ✓"
    assert report.should_proceed_with_trading() is True
    
    # API unavailable error - should still proceed
    report.status = "error"
    report.error = "API unavailable"
    assert report.should_proceed_with_trading() is True
    
    # Real error - should NOT proceed
    report.error = "Authentication failed"
    assert report.should_proceed_with_trading() is False
    
    print("✓ Reconciliation report test passed")

if __name__ == "__main__":
    test_inventory_fill()
    test_fill_state()
    test_to_float()
    test_reconciliation_report()
    print("\nAll tests passed! ✓")
