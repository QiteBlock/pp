# Duplicate Order Detection & Prevention

## Problem Fixed

Your bot was placing duplicate orders (2x Buy No at 67¢, 2x Buy Yes at 29¢). This happened because:

1. **Cancel not working properly** - `cancel_all_orders()` was either failing or not actually cancelling
2. **No duplicate detection** - Bot didn't check for existing open orders before placing new ones
3. **Race condition** - Orders placed faster than API processed cancellations

## Solution Implemented

### 1. Duplicate Detection (`src/order_duplicates.py`)

New module that:
- Fetches current open orders from Polymarket API
- Detects orders that match our quote criteria (same token, same price, same size)
- Blocks new order placement if duplicates found
- Logs all duplicate findings for debugging

**Key Features:**
- **Price tolerance** - Orders within 0.001 of target price count as duplicates
- **Size tolerance** - Orders within 0.1 shares of target size count as duplicates
- **History tracking** - All duplicate checks logged to `data/open_orders_history.jsonl`
- **API format handling** - Works with various Polymarket API response formats

### 2. Integration in Market Maker

Before placing orders, the bot now:
1. Fetches current open orders from Polymarket API
2. Checks if we already have orders at those prices/sizes
3. **Skips order placement if duplicates detected**
4. Logs the duplicate order skip event

**In code:**
```python
# Check for duplicate orders before placing new ones
if not context.dry_run:
    no_bid_price = round_no_bid_price(quote.ask, book.tick_size)
    duplicate_check = context.duplicate_detector.detect_duplicates(
        context.client,
        market.slug,
        market.yes_token_id,
        market.no_token_id,
        quote.bid,
        no_bid_price,
        quote.size,
    )
    if duplicate_check.has_duplicates:
        # Skip placing orders - duplicates already exist
        return
```

### 3. Why This Works

The duplicate detector acts as a **safety gate** between quoting logic and order placement:

```
Quote Generated → Check for Duplicates → Yes: Skip | No: Place Order
```

This means:
- Even if `cancel_all_orders()` fails, we won't place new duplicates
- Old orders get cancelled properly on next loop iteration when no duplicates found
- Bot naturally renews orders by cancelling and re-quoting without creating duplicates

## Monitoring Duplicates

### Check for Duplicate Detection Events
```bash
grep "duplicate_order_skip" data/events.jsonl
```

Output shows when duplicates blocked orders:
```json
{
  "timestamp": "2025-04-23T12:30:00+00:00",
  "event_type": "duplicate_order_skip",
  "payload": {
    "market": "eth-prediction",
    "reason": "duplicate_orders_detected",
    "duplicate_count": 2,
    "duplicates": [
      {
        "token_id": "0x123...",
        "outcome": "NO",
        "order_id": "order_1",
        "side": "BUY",
        "price": 0.67,
        "size": 20
      },
      ...
    ]
  }
}
```

### Check Duplicate Order History
```bash
tail -f data/open_orders_history.jsonl
```

Each entry shows:
- Timestamp of check
- Market and token
- How many open orders exist
- What duplicates were found

## How Cancellation Still Works

Even with duplicate detection, order cancellation still happens:

1. **Loop iteration 1:** Place orders → duplicates exist (old from crash) → skip
2. **Loop iteration 2:** Call `cancel_all_orders()` → cancels those old orders
3. **Loop iteration 3:** Check again → no duplicates now → place fresh orders

So the flow is:
- **If fresh start:** Place orders immediately (no duplicates)
- **If bot restarted/crashed:** Duplicate detector blocks re-placement until cancel runs
- **After cancel runs:** Fresh orders placed

## Configuration

No configuration needed - duplicate detection is automatic!

But you can adjust price/size tolerance in `order_duplicates.py` if needed:

```python
duplicate_check = context.duplicate_detector.detect_duplicates(
    ...,
    price_tolerance=0.001,  # Change this for stricter/looser matching
)
```

## Why You Had Duplicates

Most likely scenario:

1. Bot placed orders (Buy Yes 29¢, Buy No 67¢)
2. Bot crashed/restarted
3. On restart, old orders still existed on Polymarket
4. Reconciliation passed (positions matched)
5. **But** - order cancellation might have failed or was timing out
6. Bot placed new orders without cancelling old ones
7. Result: 2x of each order

**With this fix:**
- Step 5 would be caught by duplicate detector
- New orders blocked until cancellation succeeds
- No duplicate orders created

## Testing

Run the integration test:
```bash
python test_fill_tracking.py
```

## Future Improvements

Could enhance with:
1. **Periodic reconciliation** - Check for duplicates even between normal loops
2. **Aggressive cancel retry** - If orders still exist after cancel, keep retrying
3. **Order history** - Track all orders placed to detect anomalies
4. **Metrics** - Track how often duplicates are detected (indicator of system health)

## Key Files

| File | Purpose |
|------|---------|
| `src/order_duplicates.py` | Duplicate detection logic |
| `src/market_maker.py` | Integration into main loop |
| `data/open_orders_history.jsonl` | Log of all duplicate checks |
| `data/events.jsonl` | Events including duplicate_order_skip |

## Troubleshooting

### Still seeing duplicate orders?

1. **Check `data/events.jsonl`** for `duplicate_order_skip` events
   - If found: Detector is working, but cancellation not happening
   - If not found: Detector might not be running

2. **Verify API is responding:**
   ```bash
   grep "risk_skip" data/events.jsonl | tail -5
   ```

3. **Check cancel_all_orders is being called:**
   - Look for orders being placed once then staying
   - Check if cancel_before_requote is true in config.yaml

4. **Manual cleanup if needed:**
   - Cancel old orders via web UI
   - Delete `data/fill_state.json`
   - Restart bot

## Summary

You're now protected from placing duplicate orders through:
1. ✓ Duplicate detection before placement
2. ✓ Automatic blocking of new orders while duplicates exist
3. ✓ History tracking for debugging
4. ✓ Graceful ordering: cancel old → wait → place new

The bot runs safely even if cancellation fails occasionally - it just won't place new orders until the old ones are gone.
