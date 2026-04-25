# Fill Tracking & Reconciliation Implementation

## Overview

The bot now has robust fill tracking and startup reconciliation features to ensure safe operation at scale:

1. **Fill Poller** - Periodically fetches executed fills from Polymarket and applies them to inventory
2. **Fill State Persistence** - Tracks the last fetched timestamp in `data/fill_state.json` to avoid reprocessing
3. **Startup Reconciliation** - On startup, reconciles inventory against live Polymarket positions API to ensure consistency and detect off-chain trades

## Components

### 1. FillPoller (`src/fills.py`)

Manages periodic fill polling and inventory updates.

**Key Features:**
- Fetches fills from Polymarket API since last fetch timestamp
- Deduplicates fills to avoid applying the same fill twice
- Applies filled orders to the inventory book
- Logs all fills to `data/fills.csv` for analysis
- Persists state to `data/fill_state.json`

**How it works:**
1. On each main loop iteration (when not in dry run mode), `FillPoller.poll_fills()` is called
2. It fetches all fills since the last `last_fetch_ts`
3. For each fill, it:
   - Extracts token_id, side, size, price from the API response
   - Maps token_id to (market_slug, outcome) using the token mapping
   - Updates inventory via `InventoryBook.apply_fill()`
   - Logs the fill to CSV and events JSONL
4. Updates `last_fetch_ts` to prevent re-polling the same fills

**State File Format:**
```json
{
  "last_fetch_ts": 1234567890000,
  "last_processed_fill_id": "order_id_123"
}
```

### 2. PositionReconciler (`src/reconcile.py`)

Performs startup reconciliation against live Polymarket positions.

**Key Features:**
- Fetches current balances from Polymarket API on startup
- Detects discrepancies between inventory and API
- Automatically corrects inventory to match API state
- Identifies unknown positions (tokens not in config)
- Identifies orphaned positions (in inventory but not in API)
- Provides detailed reconciliation report

**How it works:**
1. On startup (if not in dry_run mode), `PositionReconciler.reconcile_startup()` is called
2. Fetches current user positions from Polymarket API
3. For each position:
   - Looks up the market and outcome from token_id
   - Compares API balance to inventory balance
   - If difference > 0.01 shares, logs discrepancy
   - Updates inventory to match API
4. Returns a `ReconciliationReport` with:
   - Status: "consistent", "reconciled", "error", or "no_positions_found"
   - List of discrepancies found
   - List of adjustments made
   - Any errors encountered
5. Bot only proceeds if reconciliation is successful

**Reconciliation Report:**
```python
{
  "status": "reconciled",
  "discrepancies": [
    {
      "market": "eth-prediction",
      "outcome": "YES",
      "token_id": "0x123...",
      "api_balance": 50.5,
      "inventory_balance": 50.0,
      "difference": 0.5
    }
  ],
  "adjustments": [
    {
      "market": "eth-prediction", 
      "outcome": "YES",
      "adjustment": 0.5,
      "new_balance": 50.5
    }
  ]
}
```

### 3. Enhanced PolymarketClient

New methods added to `src/client.py`:

**`get_user_fills(since_ts: Optional[int] = None) -> list[dict]`**
- Fetches fills for the authenticated user
- `since_ts` is in milliseconds since epoch (optional)
- Returns list of fill records with structure:
  ```python
  {
    "id": "fill_id",
    "token_id": "0x123...",
    "symbol": "ETH-PREDICTION-YES",
    "price": 0.65,
    "size": 100.0,
    "side": "BUY",  # or "SELL"
    "timestamp": "2025-04-23T10:30:00Z",
    # ... other fields from API
  }
  ```

**`get_user_positions() -> dict[str, float]`**
- Fetches current user positions (balance of each outcome token)
- Returns: `{token_id: balance, ...}`

## Integration with Main Bot

### Startup Flow
1. Load configuration
2. Initialize market selection
3. When running live (not dry_run):
   - Call `preflight_live_auth()` for authentication
   - Call `reconciliation_report = reconciler.reconcile_startup(markets)`
   - Log reconciliation results
   - Halt if reconciliation fails (safety check)

### Main Loop
Each iteration (when not in dry_run):
1. Build token_id → (market_slug, outcome) mapping
2. Call `fills_processed = fill_poller.poll_fills(token_mapping)`
3. Continue with normal market making logic

## Safety Features

1. **Idempotency**: Fills are tracked by ID to prevent double-processing
2. **Tolerance**: Small balance discrepancies (< 0.01 shares) are tolerated
3. **Logging**: All fills and reconciliation events are logged for audit trail
4. **State Persistence**: Last fetch timestamp persists across restarts
5. **Startup Safety**: Reconciliation on startup catches off-chain trades before trading

## Using the Features

### Normal Live Operation

Simply run the bot normally with `dry_run: false` in config.yaml:
```bash
python main.py
```

The bot will:
1. Perform startup reconciliation on first run
2. Poll for fills every loop iteration
3. Apply fills to inventory automatically
4. Log everything to `data/events.jsonl` and `data/fills.csv`

### Dry Run Mode

With `dry_run: true`:
- Fill polling is skipped
- Reconciliation is skipped
- No actual orders placed

### Checking Fill History

Fills are logged to `data/fills.csv`:
```bash
python scripts/analyze_fills.py
```

Analysis includes:
- Total fill count
- Fills by market
- Realized PnL tracking

### Checking Reconciliation History

Look in `data/events.jsonl` for `"startup_reconciliation"` events:
```json
{
  "timestamp": "2025-04-23T10:30:00+00:00",
  "event_type": "startup_reconciliation",
  "payload": {
    "status": "reconciled",
    "discrepancies_count": 1,
    "adjustments_count": 1,
    "error": null
  }
}
```

## Troubleshooting

### "Reconciliation failed - cannot proceed safely"
- Check that API credentials are correct
- Ensure you have positions on Polymarket
- Check network connectivity
- Review `data/events.jsonl` for error details

### Fills not being processed
- Check `data/fill_state.json` to see last fetch timestamp
- Verify fills exist on Polymarket (check web UI)
- Check for errors in `data/events.jsonl` with event_type="fill_poll_error"

### Inventory mismatches
- Reconciliation should catch these on startup
- If mismatch appears during runtime:
  - Stop bot
  - Delete or check `fill_state.json`
  - Restart (reconciliation will fix it)
  - Review fill logs to understand what happened

### API Method Not Available

If you see "failed to fetch fills from trading client" warnings:
- The py_clob_client version may not support the fill/position APIs
- The methods may have different names
- This is gracefully handled - bot continues but fill tracking won't work
- File an issue with py_clob_client authors to add/fix these endpoints

## Implementation Notes

### Fill API Response Handling

The implementation is flexible to handle different API response formats:
- Works with both py_clob_client v1 and v2
- Handles various field naming conventions (snake_case, camelCase)
- Gracefully handles missing API methods with warnings

### Timestamp Handling

Timestamps are converted to milliseconds internally for API compatibility:
- ISO format strings (e.g., "2025-04-23T10:30:00Z") are parsed
- Large integers (> 100 billion) are assumed to be milliseconds
- Small integers are multiplied by 1000 to get milliseconds

### Market Mapping

Token IDs are mapped to markets via the configuration:
```yaml
markets:
  - slug: eth-prediction
    yes_token_id: "0x123..."
    no_token_id: "0x456..."
```

This mapping is built dynamically in `_build_token_mapping()` and passed to the fill poller.

## Future Enhancements

Possible improvements:
1. Backfill fills from extended history on startup
2. Alert on unexpected position changes (potential security issue)
3. Periodic reconciliation (not just on startup)
4. Fill analytics dashboard
5. Automatic correction of inventory drifts mid-run with alerting
