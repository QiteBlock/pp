#!/usr/bin/env python
"""Test duplicate order detection."""

from src.order_duplicates import OrderDuplicateDetector, DuplicateCheckResult


def test_duplicate_detection():
    """Test that duplicates are properly detected."""
    detector = OrderDuplicateDetector()
    
    # Create mock response with duplicate orders
    mock_response = [
        {
            "id": "order_1",
            "token_id": "0x123",
            "side": "BUY",
            "price": 0.65,
            "size": 20.0,
        },
        {
            "id": "order_2",
            "token_id": "0x123",
            "side": "BUY",
            "price": 0.65,
            "size": 20.0,  # Duplicate!
        },
        {
            "id": "order_3",
            "token_id": "0x456",
            "side": "BUY",
            "price": 0.33,
            "size": 20.0,
        },
    ]
    
    normalized = detector._normalize_open_orders(mock_response)
    assert len(normalized) == 3
    print("✓ Order normalization works")
    
    # Find duplicates on token 0x123  
    duplicates = detector._find_duplicate_orders(
        normalized,
        token_id="0x123",
        side="BUY",
        price=0.65,
        size=20.0,
        tolerance=0.001,
    )
    assert len(duplicates) == 2, f"Expected 2 duplicates, got {len(duplicates)}"
    print("✓ Duplicate detection works")
    
    # Find with tolerance
    duplicates_tolerance = detector._find_duplicate_orders(
        normalized,
        token_id="0x123",
        side="BUY",
        price=0.6501,  # Slightly different price
        size=20.05,     # Slightly different size
        tolerance=0.01,
    )
    assert len(duplicates_tolerance) == 2
    print("✓ Price/size tolerance works")


def test_result_summary():
    """Test DuplicateCheckResult."""
    result = DuplicateCheckResult()
    assert result.summary() == "✓ No duplicates detected"
    print("✓ Result summary for clean state")
    
    result.has_duplicates = True
    result.duplicate_orders = [{"order_id": "1"}, {"order_id": "2"}]
    assert "2 duplicate" in result.summary()
    print("✓ Result summary for duplicates")
    
    result.error = "API failed"
    assert "error" in result.summary().lower()
    print("✓ Result summary for error")


def test_response_formats():
    """Test handling of different API response formats."""
    detector = OrderDuplicateDetector()
    
    # List format
    list_response = [{"id": "1"}, {"id": "2"}]
    assert len(detector._normalize_open_orders(list_response)) == 2
    print("✓ List response format")
    
    # Dict with 'orders' key
    dict_response = {"orders": [{"id": "1"}, {"id": "2"}]}
    assert len(detector._normalize_open_orders(dict_response)) == 2
    print("✓ Dict with 'orders' key")
    
    # Dict with 'data' key
    dict_response = {"data": [{"id": "1"}, {"id": "2"}]}
    assert len(detector._normalize_open_orders(dict_response)) == 2
    print("✓ Dict with 'data' key")
    
    # Empty
    assert len(detector._normalize_open_orders({})) == 0
    print("✓ Empty response")


if __name__ == "__main__":
    test_duplicate_detection()
    test_result_summary()
    test_response_formats()
    print("\nAll duplicate detection tests passed! ✓")
