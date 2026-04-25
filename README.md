# Polymarket MM

Python skeleton for a two-sided Polymarket CLOB market-making bot focused on binary YES/NO markets.

## Layout

- `config.yaml` runtime, pricing, risk, and market settings
- `main.py` bot entry point
- `src/client.py` Gamma + CLOB integration, including `py_clob_client_v2` trading hooks
- `src/market_maker.py` quote loop and orchestration
- `src/pricing.py` fair value and spread logic
- `src/inventory.py` YES/NO inventory state
- `src/risk.py` per-market and portfolio guardrails
- `src/signals.py` simple midpoint, volatility, and time-to-resolution signals
- `src/analytics.py` event and fill logging
- `scripts/find_markets.py` Gamma scan helper
- `scripts/analyze_fills.py` session summary helper

## Install

```bash
pip install -r requirements.txt
```

## Configure

1. Set wallet and CLOB API credentials in environment variables named in `config.yaml`.
2. Tune the `filters` and `runtime.auto_scan_*` settings to control market selection.
3. Keep `runtime.dry_run: true` until you have verified quoting behavior.

To create API credentials from your wallet key:

```bash
python scripts/create_api_key.py
```

## Run

```bash
python main.py
```

## Docker

Create `.env` from `.env.example`, then run:

```bash
docker compose up --build -d
```

## Telegram Control

Set these in `.env` and enable the section in `config.yaml`:

```bash
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

Supported commands:

- `/start` resumes trading
- `/stop` pauses trading and cancels live orders
- `/status` returns bot status and inventory

When enabled, the bot also pushes fill notifications and market-switch notifications to Telegram.

## Scan markets

```bash
python scripts/find_markets.py --config config.yaml
```

## Notes

- The bot can auto-scan Gamma, select one eligible market, and periodically rescan when the current target nears resolution.
- The bot currently quotes the YES token and derives NO value as `1 - YES`.
- Inventory neutrality is enforced via net YES minus NO position checks and quote skewing.
- The public market scan uses the Gamma API and filters for volume, spread, category, and time to resolution.
- Because Polymarket market metadata can vary slightly across endpoints, the Gamma normalization logic is intentionally defensive.
