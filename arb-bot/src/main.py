"""Runner for the arbitrage bot."""

from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from executor import Executor, PaperExecutor, RealExecutor, VenueConfig
from strategy import (
    LegSpec,
    PairSpec,
    Position,
    current_notional_usd,
    entry_signal_bps,
    exit_signal,
    executable_entry_bps,
)
from venues import Quote, StreamingQuoteFeed, VENUE_FETCHERS


LOG = logging.getLogger(__name__)

DEFAULT_LEDGER_CSV = os.path.join(os.path.dirname(os.path.dirname(__file__)), "arb_ledger.csv")
DEFAULT_FILLS_CSV = os.path.join(os.path.dirname(os.path.dirname(__file__)), "arb_fills.csv")
DEFAULT_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
HARD_MAX_ENTRY_NOTIONAL_USD = 2000.0
ENTRY_THRESHOLD_BPS = 75.0
EXIT_THRESHOLD_BPS = 5.0
STOP_LOSS_BPS = -20.0
MAX_HOLD_SECS = 180.0
DEFAULT_SYMBOL_CANDIDATES = [
    "GOOGL",
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "TSLA",
    "META",
    "COIN",
    "PLTR",
    "MSTR",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _fee_usd(notional_usd: float, fee_bps: float) -> float:
    return notional_usd * fee_bps / 10_000.0


def _config_string(table: dict[str, Any], key: str, default: str = "") -> str:
    value = table.get(key, default)
    return str(value).strip()


def _config_float(table: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = table.get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _config_bool(table: dict[str, Any], key: str, default: bool = False) -> bool:
    value = table.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _parse_yaml_config(config_path: Path) -> dict[str, dict[str, Any]]:
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid YAML config shape in {config_path}")
    return payload


def _load_single_venue_config(config_path: Path) -> VenueConfig:
    payload = _parse_yaml_config(config_path)
    bot = payload.get("bot") or {}
    venue = payload.get("venue") or {}
    trading = payload.get("trading") or {}
    integration = payload.get("sdk") or payload.get("ccxt") or {}

    venue_name = _config_string(venue, "kind") or config_path.stem
    credentials = {}
    endpoints = {}

    if venue_name == "extended":
        credentials = {
            "account_id": _config_string(venue, "account_id"),
            "api_key": _config_string(venue, "api_key"),
            "private_key": _config_string(venue, "private_key"),
            "public_key": _config_string(venue, "public_key"),
        }
        endpoints = {
            "api_base_url": _config_string(venue, "api_base_url"),
            "data_api_base_url": _config_string(venue, "data_api_base_url"),
            "ws_market_url": _config_string(venue, "ws_market_url"),
            "ws_account_url": _config_string(venue, "ws_account_url"),
        }
    elif venue_name == "grvt":
        credentials = {
            "account_id": _config_string(venue, "account_id"),
            "sub_account_id": _config_string(venue, "sub_account_id"),
            "api_key": _config_string(venue, "api_key"),
            "private_key": _config_string(venue, "private_key"),
            "wallet_private_key": _config_string(venue, "wallet_private_key"),
            "chain_id": _config_string(venue, "chain_id"),
        }
        endpoints = {
            "auth_base_url": _config_string(venue, "auth_base_url"),
            "market_data_base_url": _config_string(venue, "market_data_base_url"),
            "trading_base_url": _config_string(venue, "trading_base_url"),
            "market_ws_url": _config_string(venue, "market_ws_url"),
            "trading_ws_url": _config_string(venue, "trading_ws_url"),
        }
    else:
        raise RuntimeError(f"Unsupported venue config kind {venue_name} in {config_path}")

    return VenueConfig(
        venue=venue_name,
        allow_live=_config_bool(bot, "allow_live", False),
        user_agent=_config_string(bot, "user_agent", "arb-bot/0.1"),
        default_symbol=_config_string(trading, "default_symbol"),
        taker_fee_bps=_config_float(trading, "taker_fee_bps"),
        maker_fee_bps=_config_float(trading, "maker_fee_bps"),
        notional_per_trade_usd=_config_float(trading, "notional_per_trade_usd", 2000.0),
        max_position_usd=_config_float(trading, "max_position_usd", 8000.0),
        credentials=credentials,
        endpoints=endpoints,
        integration=integration,
    )


def load_venue_configs(config_dir: str) -> dict[str, VenueConfig]:
    base = Path(config_dir)
    configs = {
        "extended": _load_single_venue_config(base / "extended.yaml"),
        "grvt": _load_single_venue_config(base / "grvt.yaml"),
    }
    return configs


def _base_symbol_from_venue_symbol(venue: str, symbol: str) -> str | None:
    if venue == "extended" and symbol.endswith("_24_5-USD"):
        return symbol[: -len("_24_5-USD")]
    if venue == "grvt" and symbol.endswith("_USDT_Perp"):
        return symbol[: -len("_USDT_Perp")]
    return None


def _pair_specs_for_base(
    base_symbol: str,
    venue_configs: dict[str, VenueConfig],
) -> list[PairSpec]:
    extended_cfg = venue_configs["extended"]
    grvt_cfg = venue_configs["grvt"]
    extended_symbol = f"{base_symbol}_24_5-USD"
    grvt_symbol = f"{base_symbol}_USDT_Perp"
    notional_per_trade_usd = min(
        extended_cfg.notional_per_trade_usd,
        grvt_cfg.notional_per_trade_usd,
    )
    max_position_usd = min(
        extended_cfg.max_position_usd,
        grvt_cfg.max_position_usd,
    )
    return [
        PairSpec(
            name=f"{base_symbol}_ext_vs_grvt",
            long_leg=LegSpec(
                venue="extended",
                symbol=extended_symbol,
                fee_bps_taker=extended_cfg.taker_fee_bps,
                fee_bps_maker=extended_cfg.maker_fee_bps,
            ),
            short_leg=LegSpec(
                venue="grvt",
                symbol=grvt_symbol,
                fee_bps_taker=grvt_cfg.taker_fee_bps,
                fee_bps_maker=grvt_cfg.maker_fee_bps,
            ),
            notional_per_trade_usd=notional_per_trade_usd,
            entry_threshold_bps=ENTRY_THRESHOLD_BPS,
            exit_threshold_bps=EXIT_THRESHOLD_BPS,
            stop_loss_bps=STOP_LOSS_BPS,
            max_hold_secs=MAX_HOLD_SECS,
            max_position_usd=max_position_usd,
        ),
        PairSpec(
            name=f"{base_symbol}_grvt_vs_ext",
            long_leg=LegSpec(
                venue="grvt",
                symbol=grvt_symbol,
                fee_bps_taker=grvt_cfg.taker_fee_bps,
                fee_bps_maker=grvt_cfg.maker_fee_bps,
            ),
            short_leg=LegSpec(
                venue="extended",
                symbol=extended_symbol,
                fee_bps_taker=extended_cfg.taker_fee_bps,
                fee_bps_maker=extended_cfg.maker_fee_bps,
            ),
            notional_per_trade_usd=notional_per_trade_usd,
            entry_threshold_bps=ENTRY_THRESHOLD_BPS,
            exit_threshold_bps=EXIT_THRESHOLD_BPS,
            stop_loss_bps=STOP_LOSS_BPS,
            max_hold_secs=MAX_HOLD_SECS,
            max_position_usd=max_position_usd,
        ),
    ]


def fetch_quote(leg: LegSpec, quote_feed: StreamingQuoteFeed | None = None) -> Quote | None:
    if quote_feed is not None:
        quote = quote_feed.get_quote(leg.venue, leg.symbol)
        if quote is not None:
            return quote

    fetcher = VENUE_FETCHERS.get(leg.venue)
    if fetcher is None:
        LOG.warning("No fetcher configured for venue=%s", leg.venue)
        return None

    if leg.venue == "hl":
        dex, coin = leg.symbol.split(":", 1)
        return fetcher(coin, dex)
    return fetcher(leg.symbol)


def _liquidity_capped_qty(long_q: Quote, short_q: Quote) -> float:
    visible_sizes = [size for size in (long_q.ask_size, short_q.bid_size) if size > 0]
    if not visible_sizes:
        return float("inf")
    return 0.5 * min(visible_sizes)


def _open_pair_name(positions: dict[str, Position]) -> str | None:
    for pair_name, position in positions.items():
        if position.qty > 0:
            return pair_name
    return None


def _candidate_base_symbols(
    venue_configs: dict[str, VenueConfig],
    preferred_symbol: str | None,
    preferred_symbols_csv: str | None,
) -> list[str]:
    configured_candidates: list[str] = []
    for venue_name in ("extended", "grvt"):
        base_symbol = _base_symbol_from_venue_symbol(
            venue_name,
            venue_configs[venue_name].default_symbol,
        )
        if base_symbol:
            configured_candidates.append(base_symbol)

    candidates: list[str] = []
    if preferred_symbols_csv:
        for raw_symbol in preferred_symbols_csv.split(","):
            symbol = raw_symbol.strip().upper()
            if symbol and symbol not in candidates:
                candidates.append(symbol)
    elif preferred_symbol:
        candidates.append(preferred_symbol.upper())
    for candidate in configured_candidates + DEFAULT_SYMBOL_CANDIDATES:
        candidate_upper = candidate.upper()
        if candidate_upper not in candidates:
            candidates.append(candidate_upper)
    return candidates


def discover_pairs(
    venue_configs: dict[str, VenueConfig],
    quote_feed: StreamingQuoteFeed,
    preferred_symbol: str | None,
    preferred_symbols_csv: str | None,
    warmup_secs: float,
) -> list[PairSpec]:
    candidates = _candidate_base_symbols(
        venue_configs,
        preferred_symbol,
        preferred_symbols_csv,
    )
    failures: list[str] = []
    deadline = time.monotonic() + max(0.0, warmup_secs)

    while True:
        failures = []
        pairs: list[PairSpec] = []
        active_symbols: list[str] = []
        for base_symbol in candidates:
            pair_specs = _pair_specs_for_base(base_symbol, venue_configs)
            ext_quote = fetch_quote(pair_specs[0].long_leg, quote_feed)
            grvt_quote = fetch_quote(pair_specs[0].short_leg, quote_feed)
            if ext_quote is not None and grvt_quote is not None:
                pairs.extend(pair_specs)
                active_symbols.append(base_symbol)
                continue
            failures.append(
                f"{base_symbol}(extended={'ok' if ext_quote else 'missing'},"
                f" grvt={'ok' if grvt_quote else 'missing'})"
            )

        if pairs:
            LOG.info(
                "Scanning %d active base symbols via websocket market data: %s",
                len(active_symbols),
                ", ".join(active_symbols),
            )
            if failures:
                LOG.info("Skipping inactive symbols at startup: %s", ", ".join(failures))
            return pairs

        if time.monotonic() >= deadline:
            break
        time.sleep(0.5)

    raise RuntimeError("No live Extended/GRVT symbol available from candidates: " + ", ".join(failures))


def _write_snapshot(
    ledger_writer: csv.writer,
    ledger_fh,
    pair: PairSpec,
    pos: Position,
    long_q: Quote,
    short_q: Quote,
) -> None:
    entry_edge = executable_entry_bps(
        long_q,
        short_q,
        pair.long_leg.fee_bps_taker,
        pair.short_leg.fee_bps_taker,
    )
    ledger_writer.writerow(
        [
            _utc_now(),
            pair.name,
            pair.long_leg.venue,
            pair.long_leg.symbol,
            f"{long_q.bid:.8f}",
            f"{long_q.ask:.8f}",
            pair.short_leg.venue,
            pair.short_leg.symbol,
            f"{short_q.bid:.8f}",
            f"{short_q.ask:.8f}",
            f"{entry_edge:.4f}",
            f"{pos.qty:.8f}",
            f"{pos.long_avg_entry:.8f}",
            f"{pos.short_avg_entry:.8f}",
        ]
    )
    ledger_fh.flush()


def _close_position(
    pair: PairSpec,
    pos: Position,
    executor: Executor,
    long_q: Quote,
    short_q: Quote,
    reason: str,
) -> bool:
    if pos.qty <= 0:
        return False

    exit_qty = pos.qty
    prior_long_avg = pos.long_avg_entry
    prior_short_avg = pos.short_avg_entry

    long_fill = executor.market_order(
        pair.long_leg.venue,
        pair.long_leg.symbol,
        "sell",
        exit_qty,
        long_q,
        pair.long_leg.fee_bps_taker,
    )
    short_fill = executor.market_order(
        pair.short_leg.venue,
        pair.short_leg.symbol,
        "buy",
        exit_qty,
        short_q,
        pair.short_leg.fee_bps_taker,
    )
    if long_fill is None or short_fill is None:
        LOG.warning("Incomplete %s exit for %s, keeping position open", reason, pair.name)
        return False

    long_pnl_usd = exit_qty * (long_fill.price - prior_long_avg)
    short_pnl_usd = exit_qty * (prior_short_avg - short_fill.price)
    entry_notional_long = exit_qty * prior_long_avg
    entry_notional_short = exit_qty * prior_short_avg
    exit_notional_long = exit_qty * long_fill.price
    exit_notional_short = exit_qty * short_fill.price
    fees_usd = (
        _fee_usd(entry_notional_long, pair.long_leg.fee_bps_taker)
        + _fee_usd(entry_notional_short, pair.short_leg.fee_bps_taker)
        + _fee_usd(exit_notional_long, pair.long_leg.fee_bps_taker)
        + _fee_usd(exit_notional_short, pair.short_leg.fee_bps_taker)
    )
    net_pnl_usd = long_pnl_usd + short_pnl_usd - fees_usd
    basis_notional = max(1.0, entry_notional_long)
    realized_bps = net_pnl_usd / basis_notional * 10_000.0
    pos.realized_bps_pnl += realized_bps
    pos.qty = 0.0
    pos.long_avg_entry = 0.0
    pos.short_avg_entry = 0.0
    pos.opened_at_monotonic = 0.0
    pos.entry_edge_bps = 0.0

    LOG.info(
        "%s EXIT %s qty=%.6f long_pnl=%.2f short_pnl=%.2f fees=%.2f net=%.2f realized_bps=%.2f",
        reason.upper(),
        pair.name,
        exit_qty,
        long_pnl_usd,
        short_pnl_usd,
        fees_usd,
        net_pnl_usd,
        realized_bps,
    )
    return True


def _enter_position(
    pair: PairSpec,
    pos: Position,
    executor: Executor,
    long_q: Quote,
    short_q: Quote,
    entry_edge_bps: float,
    now_monotonic: float,
) -> None:
    if long_q.ask <= 0:
        return

    remaining_capacity = max(0.0, pair.max_position_usd - current_notional_usd(pos))
    if remaining_capacity < 100.0:
        LOG.info("%s at position cap, skipping new entry", pair.name)
        return

    target_notional = min(
        pair.notional_per_trade_usd,
        HARD_MAX_ENTRY_NOTIONAL_USD,
        remaining_capacity,
    )
    qty = target_notional / long_q.ask
    qty = min(qty, _liquidity_capped_qty(long_q, short_q))
    resulting_notional = qty * long_q.ask
    if resulting_notional < 100.0:
        LOG.info("%s size below $100 after caps, skipping", pair.name)
        return

    long_fill = executor.market_order(
        pair.long_leg.venue,
        pair.long_leg.symbol,
        "buy",
        qty,
        long_q,
        pair.long_leg.fee_bps_taker,
    )
    short_fill = executor.market_order(
        pair.short_leg.venue,
        pair.short_leg.symbol,
        "sell",
        qty,
        short_q,
        pair.short_leg.fee_bps_taker,
    )
    if long_fill is None or short_fill is None:
        LOG.warning("Incomplete entry for %s, position unchanged", pair.name)
        return

    pos.long_avg_entry = long_fill.price
    pos.short_avg_entry = short_fill.price
    pos.qty = qty
    pos.opened_at_monotonic = now_monotonic
    pos.entry_edge_bps = entry_edge_bps
    LOG.info(
        "ENTER %s qty=%.6f notional=%.2f edge_bps=%.2f long=%s@%.6f short=%s@%.6f",
        pair.name,
        qty,
        resulting_notional,
        entry_edge_bps,
        pair.long_leg.venue,
        long_fill.price,
        pair.short_leg.venue,
        short_fill.price,
    )


def _snapshot_market(
    pairs: list[PairSpec],
    positions: dict[str, Position],
    ledger_writer: csv.writer,
    ledger_fh,
    quote_feed: StreamingQuoteFeed | None = None,
) -> dict[str, tuple[Quote, Quote, float]]:
    market: dict[str, tuple[Quote, Quote, float]] = {}
    for pair in pairs:
        long_q = fetch_quote(pair.long_leg, quote_feed)
        short_q = fetch_quote(pair.short_leg, quote_feed)
        if long_q is None or short_q is None:
            missing = []
            if long_q is None:
                missing.append(f"{pair.long_leg.venue}:{pair.long_leg.symbol}")
            if short_q is None:
                missing.append(f"{pair.short_leg.venue}:{pair.short_leg.symbol}")
            LOG.warning("Skipping %s because quotes are missing for %s", pair.name, ", ".join(missing))
            continue

        _write_snapshot(ledger_writer, ledger_fh, pair, positions[pair.name], long_q, short_q)
        market[pair.name] = (
            long_q,
            short_q,
            executable_entry_bps(
                long_q,
                short_q,
                pair.long_leg.fee_bps_taker,
                pair.short_leg.fee_bps_taker,
            ),
        )
    return market


def flatten_open_positions(
    pairs: list[PairSpec],
    positions: dict[str, Position],
    executor: Executor,
    quote_feed: StreamingQuoteFeed | None = None,
) -> None:
    for pair in pairs:
        pos = positions[pair.name]
        if pos.qty <= 0:
            continue

        long_q = fetch_quote(pair.long_leg, quote_feed)
        short_q = fetch_quote(pair.short_leg, quote_feed)
        if long_q is None or short_q is None:
            missing = []
            if long_q is None:
                missing.append(f"{pair.long_leg.venue}:{pair.long_leg.symbol}")
            if short_q is None:
                missing.append(f"{pair.short_leg.venue}:{pair.short_leg.symbol}")
            LOG.warning(
                "Shutdown flatten skipped for %s because quotes are missing for %s",
                pair.name,
                ", ".join(missing),
            )
            continue

        _close_position(pair, pos, executor, long_q, short_q, "shutdown")


def _build_executor(
    live: bool,
    fills_csv: str,
    venue_configs: dict[str, VenueConfig],
    pairs: list[PairSpec],
) -> Executor:
    if not live:
        return PaperExecutor(fills_csv)

    executor = RealExecutor(fills_csv, venue_configs)
    executor.validate_pair_support(pairs)
    return executor


def main() -> None:
    parser = argparse.ArgumentParser(description="Arbitrage bot")
    parser.add_argument("--duration-min", type=float, default=60.0)
    parser.add_argument("--poll-secs", type=float, default=15.0)
    parser.add_argument("--ledger-csv", default=DEFAULT_LEDGER_CSV)
    parser.add_argument("--fills-csv", default=DEFAULT_FILLS_CSV)
    parser.add_argument("--config-dir", default=DEFAULT_CONFIG_DIR)
    parser.add_argument("--live", action="store_true", help="Enable real-order execution")
    parser.add_argument(
        "--confirm-live",
        default="",
        help="Must be set to LIVE to arm --live",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Optional stock base symbol to force, for example MSFT or NVDA",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated base symbols to scan, for example MSFT,NVDA,AAPL",
    )
    parser.add_argument(
        "--warmup-secs",
        type=float,
        default=8.0,
        help="Seconds to wait for websocket quotes before declaring startup failure",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if args.live and args.confirm_live != "LIVE":
        raise SystemExit("Refusing to arm live mode without --confirm-live LIVE")

    ledger_parent = os.path.dirname(args.ledger_csv)
    if ledger_parent:
        os.makedirs(ledger_parent, exist_ok=True)

    venue_configs = load_venue_configs(args.config_dir)
    candidate_bases = _candidate_base_symbols(venue_configs, args.symbol, args.symbols)
    quote_feed = StreamingQuoteFeed(venue_configs, candidate_bases)
    ledger_fh = None
    executor: Executor | None = None
    pairs: list[PairSpec] = []
    positions: dict[str, Position] = {}
    try:
        quote_feed.start()
        pairs = discover_pairs(
            venue_configs,
            quote_feed,
            args.symbol,
            args.symbols,
            args.warmup_secs,
        )
        positions = {pair.name: Position() for pair in pairs}
        deadline = time.monotonic() + max(0.0, args.duration_min * 60.0)

        ledger_fh = open(args.ledger_csv, "w", newline="", encoding="utf-8")
        ledger_writer = csv.writer(ledger_fh)
        ledger_writer.writerow(
            [
                "ts",
                "pair",
                "long_venue",
                "long_symbol",
                "long_bid",
                "long_ask",
                "short_venue",
                "short_symbol",
                "short_bid",
                "short_ask",
                "entry_edge_bps",
                "position_qty",
                "long_avg_entry",
                "short_avg_entry",
            ]
        )
        ledger_fh.flush()

        executor = _build_executor(args.live, args.fills_csv, venue_configs, pairs)
        while time.monotonic() < deadline:
            loop_started = time.monotonic()
            market = _snapshot_market(pairs, positions, ledger_writer, ledger_fh, quote_feed)
            open_pair_name = _open_pair_name(positions)
            now_monotonic = time.monotonic()

            if open_pair_name is not None:
                open_pair = next(pair for pair in pairs if pair.name == open_pair_name)
                open_quotes = market.get(open_pair_name)
                if open_quotes is not None:
                    long_q, short_q, _entry_edge = open_quotes
                    reason, exit_edge_bps = exit_signal(
                        open_pair,
                        positions[open_pair_name],
                        long_q,
                        short_q,
                        now_monotonic,
                    )
                    if reason is not None:
                        LOG.info(
                            "%s exit signal for %s at %.2f bps",
                            reason.upper(),
                            open_pair_name,
                            exit_edge_bps,
                        )
                        _close_position(
                            open_pair,
                            positions[open_pair_name],
                            executor,
                            long_q,
                            short_q,
                            reason,
                        )
            else:
                best_pair: PairSpec | None = None
                best_quotes: tuple[Quote, Quote] | None = None
                best_entry_edge_bps = float("-inf")
                for pair in pairs:
                    pair_quotes = market.get(pair.name)
                    if pair_quotes is None:
                        continue
                    long_q, short_q, _entry_edge = pair_quotes
                    candidate_edge_bps = entry_signal_bps(
                        pair,
                        positions[pair.name],
                        long_q,
                        short_q,
                    )
                    if candidate_edge_bps > best_entry_edge_bps:
                        best_pair = pair
                        best_quotes = (long_q, short_q)
                        best_entry_edge_bps = candidate_edge_bps

                if (
                    best_pair is not None
                    and best_quotes is not None
                    and best_entry_edge_bps >= best_pair.entry_threshold_bps
                ):
                    _enter_position(
                        best_pair,
                        positions[best_pair.name],
                        executor,
                        best_quotes[0],
                        best_quotes[1],
                        best_entry_edge_bps,
                        now_monotonic,
                    )

            sleep_for = max(0.0, args.poll_secs - (time.monotonic() - loop_started))
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        if executor is not None and pairs and positions:
            flatten_open_positions(pairs, positions, executor, quote_feed)
        if executor is not None:
            executor.close()
        if ledger_fh is not None:
            ledger_fh.close()
        quote_feed.stop()

    for pair in pairs:
        pos = positions[pair.name]
        LOG.info(
            "FINAL pair=%s qty=%.6f long_avg=%.6f short_avg=%.6f realized_bps=%.2f",
            pair.name,
            pos.qty,
            pos.long_avg_entry,
            pos.short_avg_entry,
            pos.realized_bps_pnl,
        )


if __name__ == "__main__":
    main()
