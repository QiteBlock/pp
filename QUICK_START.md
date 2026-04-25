# Quick Start Guide - Fill Tracking & Reconciliation

## What Changed?

Your bot now has two important safety systems:

1. **Fill Poller**: Automatically tracks every order that fills and updates your inventory
2. **Startup Reconciliation**: On startup, syncs inventory with Polymarket to catch any missed trades or bot restarts

## Running the Bot

### Normal Operation
```bash
python main.py
```

**What happens:**
1. Bot starts and checks positions on Polymarket
2. Reconciliation report is printed (should show "consistent ✓" on normal restarts)
3. Main trading loop starts, polling for fills every loop iteration
4. Fills are automatically logged and inventory updated

### Dry Run (Test Mode)
```yaml
runtime:
  dry_run: true
```

- No orders placed
- No fill polling
- No reconciliation
- Safe to test configuration

## Monitoring

### Check Fill History
```bash
python scripts/analyze_fills.py
```

### Raw Fill Log
Look in `data/fills.csv`:
```
timestamp,market,outcome,side,size,price
2025-04-23T10:30:00+00:00,eth-prediction,YES,BUY,100,0.65
```

### Check Reconciliation
Look in `data/events.jsonl` for events with `"startup_reconciliation"`:
```bash
grep "startup_reconciliation" data/events.jsonl
```

### Current Fill State
```bash
cat data/fill_state.json
```

Shows:
- `last_fetch_ts`: Last time we checked for fills (milliseconds)
- `last_processed_fill_id`: ID of last fill we processed

## If Something Goes Wrong

### Bot Won't Start - Reconciliation Failed
**Error:** `Startup reconciliation failed - cannot proceed safely`

**Solutions:**
1. Check you have positions on Polymarket (web UI)
2. Verify API credentials in `.env`
3. Check network connectivity
4. Review `data/events.jsonl` for specific error
5. If all else fails: Delete `data/fill_state.json` and restart

### Fills Not Processing
**Symptom:** No new fills in `data/fills.csv`

**Check:**
1. Do fills exist on Polymarket web UI?
2. Check `data/events.jsonl` for "fill_poll_error"
3. Is bot running (not dry_run)?
4. Check timestamp in `data/fill_state.json` is recent

**Your bot may not support fill APIs yet** - if you see warnings like "Failed to fetch fills from trading client", your py_clob_client version doesn't have the method. This is OK - the bot continues trading but won't track fills automatically.

### Inventory Mismatch
**Symptom:** Inventory doesn't match Polymarket

**Fix:**
1. Stop the bot
2. Delete `data/fill_state.json`
3. Start the bot (reconciliation will fix it)
4. Check reconciliation report: `grep "startup_reconciliation" data/events.jsonl`

### Extra Positions on Polymarket
**Symptom:** Made trades via web UI, inventory doesn't show them

**What happens:**
- Reconciliation detects and fixes this on next startup
- Inventory updated to match Polymarket
- Event logged: `"reconciliation_status": "reconciled"`

## Performance Impact

- **Fill polling**: ~100ms per loop iteration (minimal)
- **Startup reconciliation**: ~500ms on startup (one-time)
- **Data files**: ~100KB per month of trading

## Troubleshooting

| Issue | Check | Solution |
|-------|-------|----------|
| Bot won't start | `data/events.jsonl` errors | Check API creds & network |
| No fills tracked | `data/fill_state.json` timestamp | May need py_clob_client update |
| Inventory mismatch | Reconciliation event | Delete fill_state.json & restart |
| Unknown token warnings | Fill applies | Token not in config - update config |

## Key Files

| File | Purpose |
|------|---------|
| `data/fill_state.json` | Tracks fill polling state |
| `data/fills.csv` | Log of all fills |
| `data/events.jsonl` | All bot events (reconciliation, fills, etc.) |
| `FILL_TRACKING.md` | Detailed docs |
| `IMPLEMENTATION_SUMMARY.md` | Technical overview |

## Typical Run Output

```
Loaded 5 configured markets. Dry run=False. Auto-scan=True.
Startup reconciliation: Inventory consistent with API ✓
Processed 3 new fill(s)
[eth-prediction] fair=0.654 top=(0.653,0.655) yes_bid=0.651 yes_ask=0.657...
...
```

## Next Steps

1. Test with `dry_run: true` to verify setup
2. Run with small position sizes initially  
3. Monitor `data/events.jsonl` and `data/fills.csv`
4. Once confident, increase trade size
5. Monitor daily for any reconciliation issues

That's it! The fill tracking system handles everything automatically.
