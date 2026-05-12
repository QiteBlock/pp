from __future__ import annotations

from datetime import datetime, timezone
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional
import json
import re

import yaml

from .analytics import AnalyticsWriter
from .client import MarketConfig, PolymarketClient, OrderBookSnapshot, normalize_gamma_market, normalize_reward_market, parse_market_configs
from .fills import FillPoller
from .inventory import InventoryBook
from .order_duplicates import OrderDuplicateDetector
from .pricing import PricingConfig, Quote, build_yes_quote, clamp_probability, round_to_tick
from .reconcile import PositionReconciler
from .risk import RiskConfig, can_quote_market, exposure_by_market, total_exposure
from .signals import build_signal
from .telegram_bot import BotControl, TelegramController

EXCHANGE_MIN_ORDER_SIZE = 5.0
TOXIC_FLOW_TRADE_WINDOW = 12
TOXIC_FLOW_ONE_WAY_THRESHOLD = 0.70
TOXIC_FLOW_VOL_MULTIPLIER = 1.0
UNWIND_IOC_ESCAPE_VOL_MULTIPLIER = 1.5
UNWIND_IOC_ESCAPE_CYCLES = 2
ADAPTIVE_FILL_COOLDOWN_MILD = 45
ADAPTIVE_FILL_COOLDOWN_STRONG = 60
MERGE_GAS_FALLBACK_USDC = 1.0
STUCK_UNWIND_MAX_CYCLES = 200
BALANCE_RE = re.compile(r"balance:\s*(\d+),\s*sum of matched orders:\s*(\d+),\s*order amount[^:]*:\s*(\d+)")


@dataclass
class BotContext:
    client: PolymarketClient
    markets: list[MarketConfig]
    pricing: PricingConfig
    risk: RiskConfig
    inventory: InventoryBook
    analytics: AnalyticsWriter
    fill_poller: FillPoller
    reconciler: PositionReconciler
    duplicate_detector: OrderDuplicateDetector
    control: BotControl
    telegram: TelegramController
    loop_interval_seconds: int
    fill_cooldown_seconds: int
    dry_run: bool
    cancel_before_requote: bool
    auto_scan_enabled: bool
    force_unwind_only: bool
    scan_limit: int
    scan_pages: int
    scan_candidate_cap: int
    max_active_markets: int
    unwind_escalation_cycles: int
    reconciliation_interval_loops: int
    rescan_interval_seconds: int
    last_scan_ts: float
    active_market: Optional[MarketConfig]
    active_markets: list[MarketConfig]
    positions_to_unwind: list[MarketConfig]
    discovered_positions_path: Path
    market_cache_path: Path
    reentry_cooldown_path: Path
    post_unwind_cooldown_seconds: int
    reentry_cooldowns: dict[str, float]
    released_markets: set[str]
    exchange_pause_until: float
    exchange_pause_reason: Optional[str]
    unresolved_position_logged_at: dict[str, float]
    filters: dict[str, Any]


def run_market_maker(config_path: str) -> None:
    context = load_context(config_path)
    try:
        initialize_market_selection(context)
        print(f"Loaded {len(context.markets)} configured markets. Dry run={context.dry_run}. Auto-scan={context.auto_scan_enabled}.")
        if not context.dry_run:
            preflight_live_auth(context)
            reconciliation_report = perform_reconciliation(context, trigger="startup")
            print(f"Startup reconciliation: {reconciliation_report.summary()}")
            if not reconciliation_report.should_proceed_with_trading():
                raise RuntimeError(f"Startup reconciliation failed - cannot proceed safely: {reconciliation_report.error}")
        if context.telegram.is_enabled():
            context.telegram.send_message(build_status_message(context, prefix="Bot process online"))
        loop_count = 0
        while True:
            loop_count += 1
            handle_telegram_commands(context)
            refresh_market_selection(context)
            mark_prices: dict[str, float] = {}

            if not context.dry_run:
                token_mapping = _build_token_mapping(context)
                fills_processed = context.fill_poller.poll_fills(token_mapping)
                if fills_processed > 0:
                    print(f"Processed {fills_processed} new fill(s)")
                context.analytics.process_pending_followups(context.client)
                context.analytics.emit_periodic_summaries()
                sync_positions_to_unwind_from_inventory(context)
                if context.reconciliation_interval_loops > 0 and loop_count % context.reconciliation_interval_loops == 0:
                    reconciliation_report = perform_reconciliation(
                        context,
                        trigger="periodic",
                        loop_count=loop_count,
                    )
                    print(f"Periodic reconciliation: {reconciliation_report.summary()}")

            if not context.control.trading_enabled:
                print("Trading paused by Telegram control.")
                time.sleep(context.loop_interval_seconds)
                continue
            if context.exchange_pause_until > time.time():
                print(
                    "Trading paused by exchange state."
                    f" reason={context.exchange_pause_reason or 'unknown'}"
                )
                time.sleep(context.loop_interval_seconds)
                continue

            if context.cancel_before_requote:
                try:
                    context.client.cancel_all_orders(dry_run=context.dry_run)
                except Exception as exc:
                    context.analytics.log_event("cancel_all_error", {"details": str(exc)})
                    run_recovery_reconciliation(context, "cancel_all_error", details=str(exc))
                    print(f"Cancel-all failed: {exc}")
            markets_to_quote = get_markets_to_quote(context)
            if not markets_to_quote:
                print("No eligible market selected. Sleeping before next rescan.")
                time.sleep(context.loop_interval_seconds)
                continue
            for market in markets_to_quote:
                process_market(context, market, mark_prices)
            exposure = total_exposure(context.inventory, mark_prices)
            context.analytics.log_event(
                "portfolio_snapshot",
                {
                    "total_exposure_usdc": exposure,
                    "markets": len(mark_prices),
                    "active_markets": [market.slug for market in context.active_markets],
                    "primary_market": context.active_market.slug if context.active_market else None,
                },
            )
            if loop_count % 25 == 0:
                for market in markets_to_quote:
                    context.analytics.emit_market_pnl_snapshot(
                        market.slug,
                        context.inventory.get(market.slug),
                        mark_prices.get(market.slug),
                    )
            time.sleep(context.loop_interval_seconds)
    except Exception as exc:
        if "context" in locals() and context.telegram.is_enabled():
            context.telegram.send_message(f"Bot stopped with error\nerror={exc}")
        raise


def load_context(config_path: str) -> BotContext:
    config_file = Path(config_path)
    with config_file.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    runtime = config["runtime"]
    client = PolymarketClient(config)
    markets = parse_market_configs(config.get("markets", []))
    inventory = InventoryBook()
    analytics = AnalyticsWriter(runtime["log_dir"])
    control = BotControl(runtime["log_dir"])
    control.enable("startup:forced")
    telegram = TelegramController(config.get("telegram", {}), runtime["log_dir"])
    fill_poller = FillPoller(client, inventory, analytics, runtime["log_dir"], fill_handler=telegram.notify_fill)
    reconciler = PositionReconciler(client, inventory)
    duplicate_detector = OrderDuplicateDetector(runtime["log_dir"])
    discovered_positions_path = Path(runtime["log_dir"]) / "discovered_positions.json"
    market_cache_path = Path(runtime["log_dir"]) / "market_cache.json"
    reentry_cooldown_path = Path(runtime["log_dir"]) / "reentry_cooldowns.json"

    context = BotContext(
        client=client,
        markets=markets,
        pricing=PricingConfig(**config["pricing"]),
        risk=RiskConfig(**config["risk"]),
        inventory=inventory,
        analytics=analytics,
        fill_poller=fill_poller,
        reconciler=reconciler,
        duplicate_detector=duplicate_detector,
        control=control,
        telegram=telegram,
        loop_interval_seconds=int(runtime["loop_interval_seconds"]),
        fill_cooldown_seconds=max(int(runtime.get("fill_cooldown_seconds", 60)), 0),
        dry_run=bool(runtime["dry_run"]),
        cancel_before_requote=bool(runtime.get("cancel_before_requote", True)),
        auto_scan_enabled=bool(runtime.get("auto_scan_enabled", True)),
        force_unwind_only=bool(runtime.get("force_unwind_only", False)),
        scan_limit=int(runtime.get("scan_limit", 200)),
        scan_pages=int(runtime.get("scan_pages", 3)),
        scan_candidate_cap=int(runtime.get("scan_candidate_cap", 25)),
        max_active_markets=max(int(runtime.get("max_active_markets", 1)), 1),
        unwind_escalation_cycles=max(int(runtime.get("unwind_escalation_cycles", 3)), 1),
        reconciliation_interval_loops=max(int(runtime.get("reconciliation_interval_loops", 25)), 0),
        rescan_interval_seconds=int(runtime.get("rescan_interval_seconds", 900)),
        last_scan_ts=0.0,
        active_market=None,
        active_markets=[],
        positions_to_unwind=[],
        discovered_positions_path=discovered_positions_path,
        market_cache_path=market_cache_path,
        reentry_cooldown_path=reentry_cooldown_path,
        post_unwind_cooldown_seconds=max(int(runtime.get("post_unwind_cooldown_seconds", 1800)), 0),
        reentry_cooldowns=load_reentry_cooldowns(reentry_cooldown_path),
        released_markets=set(),
        exchange_pause_until=0.0,
        exchange_pause_reason=None,
        unresolved_position_logged_at={},
        filters=config.get("filters", {}),
    )
    hydrate_discovered_position_markets(context)
    hydrate_market_cache(context)
    return context


def hydrate_discovered_position_markets(context: BotContext) -> None:
    persisted_markets = load_discovered_position_markets(context.discovered_positions_path)
    if not persisted_markets:
        return
    merged = merge_unique_markets(context.markets, persisted_markets)
    if len(merged) != len(context.markets):
        print(f"Loaded {len(merged) - len(context.markets)} persisted position market(s)")
    context.markets = merged


def hydrate_market_cache(context: BotContext) -> None:
    cached_markets = load_cached_markets(context.market_cache_path)
    if not cached_markets:
        return
    context.markets = merge_unique_markets(context.markets, cached_markets)


def load_cached_markets(path: Path) -> list[MarketConfig]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return parse_market_configs([item for item in payload if isinstance(item, dict)])


def save_market_cache(context: BotContext) -> None:
    path = context.market_cache_path
    path.parent.mkdir(parents=True, exist_ok=True)
    merged = merge_unique_markets(load_cached_markets(path), markets_for_state_tracking(context))
    payload = [market_to_dict(market) for market in merged]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def save_market_cache_entries(context: BotContext, markets: list[MarketConfig]) -> None:
    if not markets:
        return
    cached = load_cached_markets(context.market_cache_path)
    merged = merge_unique_markets(cached, markets)
    context.market_cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [market_to_dict(market) for market in merged]
    context.market_cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_reentry_cooldowns(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, float] = {}
    now = time.time()
    for slug, expires_at in payload.items():
        try:
            expires = float(expires_at)
        except (TypeError, ValueError):
            continue
        if expires > now:
            result[str(slug)] = expires
    return result


def save_reentry_cooldowns(context: BotContext) -> None:
    prune_reentry_cooldowns(context)
    path = context.reentry_cooldown_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(context.reentry_cooldowns, indent=2), encoding="utf-8")


def prune_reentry_cooldowns(context: BotContext) -> None:
    now = time.time()
    context.reentry_cooldowns = {
        slug: expires_at
        for slug, expires_at in context.reentry_cooldowns.items()
        if expires_at > now
    }


def load_discovered_position_markets(path: Path) -> list[MarketConfig]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return parse_market_configs([item for item in payload if isinstance(item, dict)])


def save_discovered_position_markets(context: BotContext) -> None:
    path = context.discovered_positions_path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        market_to_dict(market)
        for market in context.positions_to_unwind
        if context.inventory.get(market.slug).gross_shares > 0.01
    ]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def market_to_dict(market: MarketConfig) -> dict[str, Any]:
    return {
        "slug": market.slug,
        "question": market.question,
        "yes_token_id": market.yes_token_id,
        "no_token_id": market.no_token_id,
        "end_date": market.end_date.isoformat() if market.end_date else None,
        "neg_risk": market.neg_risk,
        "condition_id": market.condition_id,
        "reward_rate_per_day": market.reward_rate_per_day,
        "rewards_min_size": market.rewards_min_size,
        "rewards_max_spread": market.rewards_max_spread,
        "market_competitiveness": market.market_competitiveness,
    }


def merge_unique_markets(*market_lists: list[MarketConfig]) -> list[MarketConfig]:
    merged: list[MarketConfig] = []
    by_slug: dict[str, int] = {}
    for markets in market_lists:
        for market in markets:
            existing_index = by_slug.get(market.slug)
            if existing_index is None:
                by_slug[market.slug] = len(merged)
                merged.append(market)
                continue
            merged[existing_index] = prefer_market_metadata(merged[existing_index], market)
    return merged


def prefer_market_metadata(current: MarketConfig, candidate: MarketConfig) -> MarketConfig:
    if not current.question and candidate.question:
        current.question = candidate.question
    if not current.yes_token_id and candidate.yes_token_id:
        current.yes_token_id = candidate.yes_token_id
    if not current.no_token_id and candidate.no_token_id:
        current.no_token_id = candidate.no_token_id
    if current.end_date is None and candidate.end_date is not None:
        current.end_date = candidate.end_date
    if current.neg_risk is None and candidate.neg_risk is not None:
        current.neg_risk = candidate.neg_risk
    if not current.condition_id and candidate.condition_id:
        current.condition_id = candidate.condition_id
    if current.reward_rate_per_day is None and candidate.reward_rate_per_day is not None:
        current.reward_rate_per_day = candidate.reward_rate_per_day
    if current.rewards_min_size is None and candidate.rewards_min_size is not None:
        current.rewards_min_size = candidate.rewards_min_size
    if current.rewards_max_spread is None and candidate.rewards_max_spread is not None:
        current.rewards_max_spread = candidate.rewards_max_spread
    if current.market_competitiveness is None and candidate.market_competitiveness is not None:
        current.market_competitiveness = candidate.market_competitiveness
    return current


def _build_token_mapping(context: BotContext) -> dict[str, tuple[str, str]]:
    """Build mapping from token_id to (market_slug, outcome).
    
    Used for mapping API fills to inventory positions.
    """
    mapping = {}
    markets = markets_for_state_tracking(context)
    for market in markets:
        mapping[market.yes_token_id] = (market.slug, "YES")
        mapping[market.no_token_id] = (market.slug, "NO")
    return mapping


def markets_for_state_tracking(context: BotContext) -> list[MarketConfig]:
    markets = list(context.markets)
    for unwind_market in context.positions_to_unwind:
        if all(market.slug != unwind_market.slug for market in markets):
            markets.append(unwind_market)
    for active_market in context.active_markets:
        if all(market.slug != active_market.slug for market in markets):
            markets.append(active_market)
    return markets


def perform_reconciliation(context: BotContext, trigger: str, **details: Any) -> Any:
    report = context.reconciler.reconcile_positions(markets_for_state_tracking(context))
    sync_positions_to_unwind(context, report)
    save_market_cache(context)
    payload = {
        "trigger": trigger,
        "status": report.status,
        "discrepancies_count": len(report.discrepancies),
        "adjustments_count": len(report.adjustments),
        "unknown_positions_count": len(report.unknown_positions),
        "orphaned_positions_count": len(report.orphaned_positions),
        "error": report.error,
    }
    payload.update(details)
    context.analytics.log_event("reconciliation", payload)
    for pnl_adjustment in getattr(report, "pnl_adjustments", []):
        context.analytics.log_event("reconcile_pnl_adjustment", pnl_adjustment)
    return report


def run_recovery_reconciliation(context: BotContext, trigger: str, **details: Any) -> None:
    if context.dry_run:
        return
    report = perform_reconciliation(context, trigger=trigger, **details)
    print(f"Recovery reconciliation ({trigger}): {report.summary()}")


def process_market(context: BotContext, market: MarketConfig, mark_prices: dict[str, float]) -> None:
    if market.slug in context.released_markets:
        return
    context.analytics.note_market_active(market.slug)
    try:
        book = context.client.get_orderbook(market.yes_token_id)
    except Exception as exc:
        context.analytics.log_event(
            "market_error",
            {"market": market.slug, "reason": "orderbook_fetch_failed", "details": str(exc)},
        )
        print(f"[{market.slug}] skipped: failed to fetch YES orderbook for token {market.yes_token_id}")
        return
    if market.neg_risk is None:
        market.neg_risk = context.client.get_neg_risk(market.yes_token_id)
    recent_prices = context.client.fetch_market_history(market.yes_token_id)
    signal = build_signal(book.midpoint, book.last_trade_price, recent_prices, market.end_date)
    inventory = context.inventory.get(market.slug)
    apply_adaptive_fill_cooldown(context, inventory, signal)
    if is_unwind_priority_market(context, market) and inventory.gross_shares > 0.01:
        process_unwind_market(
            context,
            market,
            inventory,
            build_yes_quote(
                context.pricing,
                signal,
                inventory.net_delta,
                book.tick_size,
                book.spread,
                best_bid=book.best_bid,
                best_ask=book.best_ask,
            ),
            book,
            "priority position unwind",
            mode="flatten",
            aggressive=False,
            signal=signal,
        )
        inventory.last_mark_price = quote_safe_mark(signal)
        return
    inventory_bias = inventory.net_delta
    drawdown_ratio = current_drawdown_ratio(context, inventory)
    pricing_config = effective_pricing_config(context, drawdown_ratio)
    quote = build_yes_quote(
        pricing_config,
        signal,
        inventory_bias,
        book.tick_size,
        book.spread,
        best_bid=book.best_bid,
        best_ask=book.best_ask,
    )
    quote.size = derive_quote_size(context, market, quote.size)
    mark_prices[market.slug] = quote.fair_value
    inventory.last_mark_price = quote.fair_value
    fair_value_breach = fair_value_band_breach(quote.fair_value, context.filters)
    if fair_value_breach:
        if inventory.gross_shares > 0.01:
            process_unwind_market(
                context,
                market,
                inventory,
                quote,
                book,
                fair_value_breach,
                mode="flatten",
                aggressive=False,
                signal=signal,
            )
            return
        context.analytics.log_event(
            "risk_skip",
            {"market": market.slug, "reason": fair_value_breach, "fair_value": quote.fair_value},
        )
        context.analytics.note_skip_reason(market.slug, fair_value_breach)
        print(f"[{market.slug}] skipped: {fair_value_breach} fair={quote.fair_value:.3f}")
        return
    portfolio_exposure = total_exposure(context.inventory, mark_prices)
    contribution_map = exposure_by_market(context.inventory, mark_prices)
    largest_contributor = max(contribution_map, key=contribution_map.get, default=None)
    if drawdown_ratio >= 0.75:
        if inventory.gross_shares > 0.01:
            process_unwind_market(
                context,
                market,
                inventory,
                quote,
                book,
                "drawdown unwind-only threshold",
                mode="flatten",
                aggressive=False,
                signal=signal,
            )
            return
        context.analytics.log_event(
            "risk_skip",
            {"market": market.slug, "reason": "drawdown unwind-only threshold", "drawdown_ratio": drawdown_ratio},
        )
        context.analytics.note_skip_reason(market.slug, "drawdown unwind-only threshold")
        print(f"[{market.slug}] skipped: drawdown unwind-only threshold ratio={drawdown_ratio:.2f}")
        return
    allowed, reason = can_quote_market(context.risk, inventory, quote.fair_value, signal.time_to_resolution_days)
    if not allowed:
        if (
            should_trim_position(reason)
            and portfolio_exposure > context.risk.max_total_exposure_usdc
            and largest_contributor == market.slug
        ):
            process_unwind_market(
                context,
                market,
                inventory,
                quote,
                book,
                f"{reason} escalated by portfolio exposure",
                mode="flatten",
                aggressive=True,
                signal=signal,
            )
            return
        if should_trim_position(reason):
            process_unwind_market(context, market, inventory, quote, book, reason, mode="trim", aggressive=False, signal=signal)
            return
        if should_flatten_position(reason):
            process_unwind_market(
                context,
                market,
                inventory,
                quote,
                book,
                reason,
                mode="flatten",
                aggressive=(reason == "drawdown limit breached"),
                signal=signal,
            )
            return
        context.analytics.log_event("risk_skip", {"market": market.slug, "reason": reason})
        context.analytics.note_skip_reason(market.slug, reason)
        print(
            f"[{market.slug}] skipped: {reason} "
            f"yes={inventory.yes_shares:.4f} no={inventory.no_shares:.4f} delta={inventory.net_delta:.4f}"
        )
        return
    if portfolio_exposure > context.risk.max_total_exposure_usdc:
        if inventory.gross_shares > 0.01:
            process_unwind_market(
                context,
                market,
                inventory,
                quote,
                book,
                "portfolio exposure limit breached",
                mode="flatten",
                aggressive=False,
                signal=signal,
            )
            return
        context.analytics.log_event(
            "risk_skip",
            {"market": market.slug, "reason": "portfolio exposure limit breached", "exposure": portfolio_exposure},
        )
        context.analytics.note_skip_reason(market.slug, "portfolio exposure limit breached")
        print(f"[{market.slug}] skipped: portfolio exposure limit breached exposure={portfolio_exposure:.4f}")
        return
    reward_spread_breach = reward_spread_limit_breach(market, quote)
    if reward_spread_breach is not None:
        context.analytics.log_event(
            "risk_skip",
            {"market": market.slug, "reason": reward_spread_breach, "fair_value": quote.fair_value, "half_spread": quote.half_spread},
        )
        context.analytics.note_skip_reason(market.slug, reward_spread_breach)
        print(f"[{market.slug}] skipped: {reward_spread_breach}")
        return
    if not passes_post_only_guard(book, quote.bid, quote.ask):
        adjusted = adjust_quote_to_post_only(book, quote)
        if adjusted and passes_post_only_guard(book, quote.bid, quote.ask):
            context.analytics.log_event(
                "quote_adjusted_post_only",
                {
                    "market": market.slug,
                    "best_bid": book.best_bid,
                    "best_ask": book.best_ask,
                    "quote_bid": quote.bid,
                    "quote_ask": quote.ask,
                },
            )
        else:
            context.analytics.log_event(
                "risk_skip",
                {
                    "market": market.slug,
                    "reason": "post_only_guard",
                    "best_bid": book.best_bid,
                    "best_ask": book.best_ask,
                    "quote_bid": quote.bid,
                    "quote_ask": quote.ask,
                },
            )
            context.analytics.note_skip_reason(market.slug, "post_only_guard")
            print(f"[{market.slug}] skipped: post-only guard blocked marketable quote ({quote.bid:.3f},{quote.ask:.3f})")
            return
    place_yes_bid, place_no_bid, quote_reason = determine_quote_sides(context, market, inventory, signal, book)
    reward_status = evaluate_reward_eligibility(market, quote, place_both_sides=(place_yes_bid and place_no_bid))
    if not place_yes_bid and not place_no_bid:
        context.analytics.log_event(
            "risk_skip",
            {
                "market": market.slug,
                "reason": quote_reason,
                "inventory_yes": inventory.yes_shares,
                "inventory_no": inventory.no_shares,
                "last_fill_ts": inventory.last_fill_ts,
            },
        )
        context.analytics.note_skip_reason(market.slug, quote_reason)
        print(f"[{market.slug}] skipped: {quote_reason}")
        return

    no_bid_price = round_no_bid_price(quote.ask, book.tick_size)
    bid_response: Any = None
    ask_response: Any = None

    existing_yes_orders, existing_no_orders = fetch_market_open_orders(context, market)
    if not context.dry_run:
        keep_yes = (not place_yes_bid) or should_keep_existing_quotes(existing_yes_orders, quote.bid, quote.size)
        keep_no = (not place_no_bid) or should_keep_existing_quotes(existing_no_orders, no_bid_price, quote.size)
        if keep_yes and keep_no and (existing_yes_orders or existing_no_orders):
            context.analytics.log_event(
                "quote_skip_existing_orders",
                {
                    "market": market.slug,
                    "yes_price": quote.bid,
                    "no_price": no_bid_price,
                    "size": quote.size,
                    "yes_open_orders": len(existing_yes_orders),
                    "no_open_orders": len(existing_no_orders),
                },
            )
            print(
                f"[{market.slug}] kept existing quotes "
                f"yes={quote.bid:.3f} no={no_bid_price:.3f} size={quote.size:.4f}"
            )
            print_open_orders_from_lists(existing_yes_orders, existing_no_orders)
            return
        if not cancel_market_orders_for_requote(context, market, existing_yes_orders, existing_no_orders):
            context.analytics.log_event(
                "risk_skip",
                {"market": market.slug, "reason": "cancel_failed_before_requote"},
            )
            context.analytics.note_skip_reason(market.slug, "cancel_failed_before_requote")
            print(f"[{market.slug}] skipped: cancel failed before requote")
            return

    # Check for duplicate orders before placing new ones
    if not context.dry_run and place_yes_bid and place_no_bid:
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
            context.analytics.log_event(
                "duplicate_order_skip",
                {
                    "market": market.slug,
                    "reason": "duplicate_orders_detected",
                    "duplicate_count": len(duplicate_check.duplicate_orders),
                    "duplicates": duplicate_check.duplicate_orders,
                },
            )
            print(f"[{market.slug}] skipped: found {len(duplicate_check.duplicate_orders)} duplicate order(s)")
            return

    if place_yes_bid:
        try:
            bid_response = context.client.place_limit_order(
                token_id=market.yes_token_id,
                side="BUY",
                price=quote.bid,
                size=quote.size,
                tick_size=book.tick_size,
                dry_run=context.dry_run,
                neg_risk=market.neg_risk,
            )
        except Exception as exc:
            if is_trading_restricted_error(exc):
                raise RuntimeError(
                    "Polymarket rejected live order placement due to regional trading restrictions. "
                    "The bot has stopped before placing further orders. Use dry_run=true unless you are in an eligible region."
                ) from exc
            if is_cancel_only_error(exc):
                pause_for_cancel_only(context, market.slug, exc)
                print(f"[{market.slug}] YES bid skipped: exchange in cancel-only mode")
                return
            context.analytics.log_event(
                "quote_error",
                {
                    "market": market.slug,
                    "details": str(exc),
                    "fair_value": quote.fair_value,
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "side": "YES_BID",
                },
            )
            run_recovery_reconciliation(context, "quote_error", market=market.slug, details=str(exc), side="YES_BID")
            print(f"[{market.slug}] YES bid placement failed: {exc}")
    if place_no_bid:
        try:
            ask_response = context.client.place_limit_order(
                token_id=market.no_token_id,
                side="BUY",
                price=no_bid_price,
                size=quote.size,
                tick_size=book.tick_size,
                dry_run=context.dry_run,
                neg_risk=market.neg_risk,
            )
        except Exception as exc:
            if is_trading_restricted_error(exc):
                raise RuntimeError(
                    "Polymarket rejected live order placement due to regional trading restrictions. "
                    "The bot has stopped before placing further orders. Use dry_run=true unless you are in an eligible region."
                ) from exc
            if is_cancel_only_error(exc):
                pause_for_cancel_only(context, market.slug, exc)
                print(f"[{market.slug}] NO bid skipped: exchange in cancel-only mode")
                return
            context.analytics.log_event(
                "quote_error",
                {
                    "market": market.slug,
                    "details": str(exc),
                    "fair_value": quote.fair_value,
                    "bid": quote.bid,
                    "ask": quote.ask,
                    "side": "NO_BID",
                },
            )
            run_recovery_reconciliation(context, "quote_error", market=market.slug, details=str(exc), side="NO_BID")
            print(f"[{market.slug}] NO bid placement failed: {exc}")

    if bid_response is None and ask_response is None:
        return
    if bid_response is not None:
        record_order_placement_event(
            context,
            bid_response,
            market=market,
            side="BUY",
            outcome="YES",
            price=quote.bid,
            size=quote.size,
            reward_eligible=reward_status["eligible"],
            reward_rate_per_day=market.reward_rate_per_day,
            fair_value=quote.fair_value,
            placement_mark=book.midpoint if book.midpoint is not None else quote.fair_value,
        )
    if ask_response is not None:
        record_order_placement_event(
            context,
            ask_response,
            market=market,
            side="BUY",
            outcome="NO",
            price=no_bid_price,
            size=quote.size,
            reward_eligible=reward_status["eligible"],
            reward_rate_per_day=market.reward_rate_per_day,
            fair_value=quote.fair_value,
            placement_mark=book.midpoint if book.midpoint is not None else quote.fair_value,
        )
    context.analytics.log_event(
        "quote_cycle",
        {
            "market": market.slug,
            "question": market.question,
            "fair_value": quote.fair_value,
            "bid": quote.bid,
            "ask": quote.ask,
            "no_bid": no_bid_price,
            "half_spread": quote.half_spread,
            "inventory_yes": inventory.yes_shares,
            "inventory_no": inventory.no_shares,
            "place_yes_bid": place_yes_bid,
            "place_no_bid": place_no_bid,
            "quote_side_reason": quote_reason,
            "reward_rate_per_day": market.reward_rate_per_day,
            "rewards_min_size": market.rewards_min_size,
            "rewards_max_spread": normalize_reward_spread_limit(market.rewards_max_spread),
            "reward_eligible_now": reward_status["eligible"],
            "reward_eligibility_reason": reward_status["reason"],
            "responses": {"bid": bid_response, "ask": ask_response},
        },
    )
    if bid_response is not None:
        log_rejected_order_response(context, market, "YES bid", bid_response, kind="quote")
    if ask_response is not None:
        log_rejected_order_response(context, market, "NO bid", ask_response, kind="quote")
    print_quote_status(market, book, quote.fair_value, quote.bid, quote.ask, inventory.net_delta)
    if bid_response is not None:
        print_order_response("YES bid", bid_response)
    else:
        print("  YES bid: skipped")
    if ask_response is not None:
        print_order_response("NO bid", ask_response)
    else:
        print("  NO bid: skipped")
    if market.reward_rate_per_day:
        print(
            "  rewards:"
            f" min_size={market.rewards_min_size}"
            f" max_spread={normalize_reward_spread_limit(market.rewards_max_spread)}"
            f" eligible={reward_status['eligible']}"
            f" reason={reward_status['reason']}"
        )
    print_open_orders(context, market)


def process_unwind_market(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    quote: Any,
    yes_book: OrderBookSnapshot,
    reason: str,
    mode: str,
    aggressive: bool,
    signal: Any,
) -> None:
    if inventory.balance_retry_pending:
        if inventory.balance_retry_deadline > time.time():
            context.analytics.log_event(
                "unwind_skip",
                {
                    "market": market.slug,
                    "reason": "balance_retry_pending",
                    "mode": mode,
                    "aggressive": aggressive,
                    "retry_deadline": inventory.balance_retry_deadline,
                },
            )
            context.analytics.note_skip_reason(market.slug, "balance_retry_pending")
            return
        inventory.balance_retry_pending = False
        inventory.balance_retry_deadline = 0.0
    if reason == "drawdown limit breached":
        context.control.disable("risk:drawdown")
        context.analytics.log_event("drawdown_kill_switch", {"market": market.slug, "reason": reason})
        if context.telegram.is_enabled():
            context.telegram.send_message(
                "Drawdown kill switch triggered\n"
                f"market={market.slug}\n"
                f"reason={reason}\n"
                "trading paused, attempting unwind"
            )
    if mode == "flatten":
        merge_result = attempt_merge_before_flatten(context, market, inventory, reason)
        if merge_result == "merged_all":
            return
    effective_aggressive = mode == "flatten" or aggressive or (
        mode == "trim" and inventory.unwind_cycles_without_fill >= context.unwind_escalation_cycles
    )
    force_ioc_escape = should_trigger_unwind_ioc_escape(inventory, signal, effective_aggressive)
    order_request = build_unwind_order_request(
        context,
        market,
        inventory,
        quote,
        yes_book,
        mode=mode,
        aggressive=effective_aggressive,
        signal=signal,
        force_ioc_escape=force_ioc_escape,
    )
    if order_request is None:
        maybe_handle_dust_position(context, market, inventory)
        context.analytics.log_event(
            "unwind_skip",
            {
                "market": market.slug,
                "reason": reason,
                "mode": mode,
                "aggressive": effective_aggressive,
                "inventory_yes": inventory.yes_shares,
                "inventory_no": inventory.no_shares,
                "delta": inventory.net_delta,
            },
        )
        context.analytics.note_skip_reason(market.slug, f"unwind:{reason}")
        print(
            f"[{market.slug}] skipped: unable to build unwind order "
            f"yes={inventory.yes_shares:.4f} no={inventory.no_shares:.4f} delta={inventory.net_delta:.4f}"
        )
        return

    if not context.dry_run and order_request["side"].upper() == "SELL":
        yes_orders, no_orders = fetch_market_open_orders(context, market)
        if not cancel_market_orders_for_requote(context, market, yes_orders, no_orders):
            context.analytics.log_event(
                "unwind_skip",
                {
                    "market": market.slug,
                    "reason": "cancel_failed_before_unwind",
                    "mode": mode,
                    "aggressive": effective_aggressive,
                    "label": order_request["label"],
                },
            )
            context.analytics.note_skip_reason(market.slug, "cancel_failed_before_unwind")
            print(f"[{market.slug}] skipped: cancel failed before {order_request['label']}")
            return
        refresh_sell_unwind_order(context, order_request, aggressive=effective_aggressive)

    if not context.dry_run and order_request["side"].upper() == "SELL":
        if not prepare_sell_unwind_order(context, market, inventory, order_request):
            if inventory.balance_retry_pending and inventory.balance_retry_deadline > time.time():
                context.analytics.note_skip_reason(market.slug, "balance_retry_pending")
                return
            recovered = maybe_handle_dust_position(context, market, inventory)
            if not recovered:
                inventory.unwind_failed_cycles += 1
                if inventory.unwind_failed_cycles >= STUCK_UNWIND_MAX_CYCLES:
                    release_stuck_unwind_market(context, market, inventory)
            return

    try:
        response = context.client.place_limit_order(
            token_id=order_request["token_id"],
            side=order_request["side"],
            price=order_request["price"],
            size=order_request["size"],
            tick_size=order_request["tick_size"],
            dry_run=context.dry_run,
            neg_risk=market.neg_risk,
            post_only=not order_request.get("ioc_escape", False) and not effective_aggressive,
            order_type=order_request.get("order_type", "GTC"),
        )
    except Exception as exc:
        if order_request["side"].upper() == "SELL" and is_crosses_book_error(exc):
            retry_response = retry_unwind_with_safe_price(
                context,
                market,
                inventory,
                order_request,
                reason,
                mode,
                details=str(exc),
            )
            if retry_response is not None:
                response = retry_response
                context.analytics.log_event(
                    "unwind_quote_cycle",
                    {
                        "market": market.slug,
                        "reason": reason,
                        "mode": mode,
                        "aggressive": False,
                        "inventory_yes": inventory.yes_shares,
                        "inventory_no": inventory.no_shares,
                        "delta": inventory.net_delta,
                        "order_request": order_request,
                        "response": response,
                        "retry": "safe_post_only_after_cross",
                    },
                )
                log_rejected_order_response(
                    context,
                    market,
                    order_request["label"],
                    response,
                    kind="unwind",
                    mode=mode,
                    aggressive=False,
                    retry="safe_post_only_after_cross",
                )
                print(
                    f"[{market.slug}] unwind retried with safe price "
                    f"label={order_request['label']} price={order_request['price']:.3f} size={order_request['size']:.4f}"
                )
                if mode == "trim":
                    inventory.unwind_cycles_without_fill += 1
                print_order_response(order_request["label"], response)
                print_open_orders(context, market)
                return
        context.analytics.log_event(
            "unwind_error",
            {
                "market": market.slug,
                "reason": reason,
                "mode": mode,
                "aggressive": effective_aggressive,
                "order_request": order_request,
                "details": str(exc),
            },
        )
        if is_cancel_only_error(exc):
            pause_for_cancel_only(context, market.slug, exc)
            print(f"[{market.slug}] unwind skipped: exchange in cancel-only mode")
            return
        if order_request["side"].upper() == "SELL" and is_insufficient_balance_error(exc):
            recover_from_sell_balance_rejection(context, market, inventory, order_request, reason, mode, str(exc))
            run_recovery_reconciliation(
                context,
                "unwind_error",
                market=market.slug,
                reason=reason,
                mode=mode,
                details=str(exc),
            )
            return
        run_recovery_reconciliation(
            context,
            "unwind_error",
            market=market.slug,
            reason=reason,
            mode=mode,
            details=str(exc),
        )
        print(f"[{market.slug}] unwind placement failed: {exc}")
        return

    context.analytics.log_event(
        "unwind_quote_cycle",
        {
            "market": market.slug,
            "reason": reason,
            "mode": mode,
            "aggressive": effective_aggressive,
            "inventory_yes": inventory.yes_shares,
            "inventory_no": inventory.no_shares,
            "delta": inventory.net_delta,
            "order_request": order_request,
            "response": response,
        },
    )
    if response_is_success(response):
        inventory.last_mark_price = quote_safe_mark(signal)
        record_order_placement_event(
            context,
            response,
            market=market,
            side=order_request["side"],
            outcome="YES" if order_request["token_id"] == market.yes_token_id else "NO",
            price=order_request["price"],
            size=order_request["size"],
            reward_eligible=False,
            reward_rate_per_day=market.reward_rate_per_day,
            fair_value=quote.fair_value,
            placement_mark=quote_safe_mark(signal),
        )
    log_rejected_order_response(
        context,
        market,
        order_request["label"],
        response,
        kind="unwind",
        mode=mode,
        reason=reason,
    )
    print(
        f"[{market.slug}] unwind-only: mode={mode} aggressive={effective_aggressive} reason={reason} "
        f"placing {order_request['label']} price={order_request['price']:.3f} size={order_request['size']:.4f}"
    )
    if (
        mode == "flatten"
        and order_request["side"].upper() == "SELL"
        and response_is_success(response)
        and context.post_unwind_cooldown_seconds > 0
    ):
        context.reentry_cooldowns[market.slug] = time.time() + context.post_unwind_cooldown_seconds
        save_reentry_cooldowns(context)
        context.analytics.log_event(
            "market_reentry_cooldown",
            {
                "market": market.slug,
                "reason": reason,
                "cooldown_seconds": context.post_unwind_cooldown_seconds,
            },
        )
    if response_is_immediate_execution(response):
        inventory.unwind_cycles_without_fill = 0
    elif effective_aggressive or mode == "trim":
        inventory.unwind_cycles_without_fill += 1
    print_order_response(order_request["label"], response)
    print_open_orders(context, market)


def build_unwind_order_request(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    quote: Any,
    yes_book: OrderBookSnapshot,
    mode: str,
    aggressive: bool,
    signal: Any,
    force_ioc_escape: bool,
) -> Optional[dict[str, Any]]:
    tick = max(yes_book.tick_size, 0.01)
    if mode == "flatten":
        return build_flatten_order_request(context, market, inventory, quote, yes_book, aggressive, force_ioc_escape)
    if inventory.no_shares > max(context.risk.max_no_shares_per_market, context.risk.max_net_delta_per_market):
        no_book = get_market_book(context, market.no_token_id)
        if no_book is None:
            return None
        size = bounded_unwind_size(
            requested_size=context.pricing.size_per_order,
            available_size=inventory.no_shares,
            min_order_size=context.risk.min_order_size,
        )
        if size is None:
            return None
        return {
            "token_id": market.no_token_id,
            "side": "SELL",
            "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive, ioc_escape=force_ioc_escape),
            "size": size,
            "tick_size": no_book.tick_size,
            "label": "NO trim ask",
            "order_type": "FAK" if force_ioc_escape else "GTC",
            "ioc_escape": force_ioc_escape,
        }
    if inventory.yes_shares > max(context.risk.max_yes_shares_per_market, context.risk.max_net_delta_per_market):
        size = bounded_unwind_size(
            requested_size=context.pricing.size_per_order,
            available_size=inventory.yes_shares,
            min_order_size=context.risk.min_order_size,
        )
        if size is None:
            return None
        return {
            "token_id": market.yes_token_id,
            "side": "SELL",
            "price": derive_sell_price(yes_book, quote.fair_value, quote.half_spread, aggressive=aggressive, ioc_escape=force_ioc_escape),
            "size": size,
            "tick_size": tick,
            "label": "YES trim ask",
            "order_type": "FAK" if force_ioc_escape else "GTC",
            "ioc_escape": force_ioc_escape,
        }
    if inventory.net_delta < -context.risk.max_net_delta_per_market and inventory.no_shares > 0:
        no_book = get_market_book(context, market.no_token_id)
        if no_book is None:
            return None
        size = bounded_unwind_size(
            requested_size=min(
                context.pricing.size_per_order,
                abs(inventory.net_delta) - context.risk.max_net_delta_per_market,
            ),
            available_size=inventory.no_shares,
            min_order_size=context.risk.min_order_size,
        )
        if size is None:
            return None
        return {
            "token_id": market.no_token_id,
            "side": "SELL",
            "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive, ioc_escape=force_ioc_escape),
            "size": size,
            "tick_size": no_book.tick_size,
            "label": "NO trim ask",
            "order_type": "FAK" if force_ioc_escape else "GTC",
            "ioc_escape": force_ioc_escape,
        }
    if inventory.net_delta > context.risk.max_net_delta_per_market and inventory.yes_shares > 0:
        size = bounded_unwind_size(
            requested_size=min(
                context.pricing.size_per_order,
                inventory.net_delta - context.risk.max_net_delta_per_market,
            ),
            available_size=inventory.yes_shares,
            min_order_size=context.risk.min_order_size,
        )
        if size is None:
            return None
        return {
            "token_id": market.yes_token_id,
            "side": "SELL",
            "price": derive_sell_price(yes_book, quote.fair_value, quote.half_spread, aggressive=aggressive, ioc_escape=force_ioc_escape),
            "size": size,
            "tick_size": tick,
            "label": "YES trim ask",
            "order_type": "FAK" if force_ioc_escape else "GTC",
            "ioc_escape": force_ioc_escape,
        }
    return None


def build_flatten_order_request(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    quote: Any,
    yes_book: OrderBookSnapshot,
    aggressive: bool,
    force_ioc_escape: bool,
) -> Optional[dict[str, Any]]:
    yes_notional = inventory.yes_shares * quote.fair_value
    no_notional = inventory.no_shares * (1.0 - quote.fair_value)
    unwind_no_first = inventory.no_shares > 0 and no_notional >= yes_notional

    if unwind_no_first:
        no_book = get_market_book(context, market.no_token_id)
        if no_book is not None:
            size = bounded_unwind_size(
                requested_size=inventory.no_shares,
                available_size=inventory.no_shares,
                min_order_size=context.risk.min_order_size,
            )
            if size is not None:
                return {
                    "token_id": market.no_token_id,
                    "side": "SELL",
                    "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive, ioc_escape=force_ioc_escape),
                    "size": size,
                    "tick_size": no_book.tick_size,
                    "label": "NO flatten ask",
                    "order_type": "FAK" if force_ioc_escape else "GTC",
                    "ioc_escape": force_ioc_escape,
                }

    if inventory.yes_shares > 0:
        size = bounded_unwind_size(
            requested_size=inventory.yes_shares,
            available_size=inventory.yes_shares,
            min_order_size=context.risk.min_order_size,
        )
        if size is None:
            return None
        return {
            "token_id": market.yes_token_id,
            "side": "SELL",
            "price": derive_sell_price(yes_book, quote.fair_value, quote.half_spread, aggressive=aggressive, ioc_escape=force_ioc_escape),
            "size": size,
            "tick_size": max(yes_book.tick_size, 0.01),
            "label": "YES flatten ask",
            "order_type": "FAK" if force_ioc_escape else "GTC",
            "ioc_escape": force_ioc_escape,
        }

    if inventory.no_shares > 0:
        no_book = get_market_book(context, market.no_token_id)
        if no_book is None:
            return None
        size = bounded_unwind_size(
            requested_size=inventory.no_shares,
            available_size=inventory.no_shares,
            min_order_size=context.risk.min_order_size,
        )
        if size is None:
            return None
        return {
            "token_id": market.no_token_id,
            "side": "SELL",
            "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive, ioc_escape=force_ioc_escape),
            "size": size,
            "tick_size": no_book.tick_size,
            "label": "NO flatten ask",
            "order_type": "FAK" if force_ioc_escape else "GTC",
            "ioc_escape": force_ioc_escape,
        }
    return None


def bounded_unwind_size(requested_size: float, available_size: float, min_order_size: float) -> Optional[float]:
    available = max(float(available_size), 0.0)
    requested = max(float(requested_size), 0.0)
    if available <= 0.0 or requested <= 0.0:
        return None
    size = min(requested, available)
    residual = max(available - size, 0.0)
    if available >= EXCHANGE_MIN_ORDER_SIZE and 1.0 < residual < EXCHANGE_MIN_ORDER_SIZE:
        return available
    return size


def prepare_sell_unwind_order(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    order_request: dict[str, Any],
) -> bool:
    live_balance = get_live_conditional_balance(context, order_request["token_id"])
    if live_balance is None:
        context.analytics.log_event(
            "unwind_skip",
            {
                "market": market.slug,
                "reason": "conditional balance unavailable for sell unwind",
                "token_id": order_request["token_id"],
                "requested_size": order_request["size"],
                "label": order_request["label"],
            },
        )
        print(f"[{market.slug}] skipped: {order_request['label']} conditional balance unavailable, retry next cycle")
        return False
    sync_inventory_balance(market, inventory, order_request["token_id"], live_balance)
    adjusted_size = bounded_unwind_size(
        requested_size=min(order_request["size"], live_balance),
        available_size=live_balance,
        min_order_size=context.risk.min_order_size,
    )
    if adjusted_size is None:
        context.analytics.log_event(
            "unwind_skip",
            {
                "market": market.slug,
                "reason": "live balance unavailable for sell unwind size",
                "token_id": order_request["token_id"],
                "requested_size": order_request["size"],
                "live_balance": live_balance,
                "label": order_request["label"],
            },
        )
        print(f"[{market.slug}] skipped: {order_request['label']} no available live balance to sell")
        return False
    if adjusted_size + 1e-9 < EXCHANGE_MIN_ORDER_SIZE:
        context.analytics.log_event(
            "unwind_skip",
            {
                "market": market.slug,
                "token_id": order_request["token_id"],
                "reason": "sell unwind residual below exchange minimum",
                "label": order_request["label"],
                "requested_size": order_request["size"],
                "adjusted_size": adjusted_size,
                "exchange_min_order_size": EXCHANGE_MIN_ORDER_SIZE,
            },
        )
        print(
            f"[{market.slug}] skipped: {order_request['label']} residual size "
            f"{adjusted_size:.4f} below exchange minimum {EXCHANGE_MIN_ORDER_SIZE:.0f}"
        )
        return False
    if adjusted_size + 1e-9 < order_request["size"]:
        context.analytics.log_event(
            "unwind_size_adjusted",
            {
                "market": market.slug,
                "token_id": order_request["token_id"],
                "label": order_request["label"],
                "requested_size": order_request["size"],
                "adjusted_size": adjusted_size,
                "live_balance": live_balance,
            },
        )
        order_request["size"] = adjusted_size
    return True


def get_live_conditional_balance(context: BotContext, token_id: str) -> Optional[float]:
    last_error: Optional[str] = None
    for attempt in range(2):
        try:
            payload = context.client.get_conditional_balance_allowance(token_id)
            return parse_usdc_amount(payload.get("balance"))
        except Exception as exc:
            last_error = str(exc)
            context.analytics.log_event(
                "conditional_balance_check_error",
                {"token_id": token_id, "attempt": attempt + 1, "details": last_error},
            )
            time.sleep(0.5)
    return None


def sync_inventory_balance(market: MarketConfig, inventory: Any, token_id: str, live_balance: float) -> None:
    normalized_balance = max(float(live_balance), 0.0)
    if token_id == market.yes_token_id:
        inventory.yes_shares = normalized_balance
        return
    if token_id == market.no_token_id:
        inventory.no_shares = normalized_balance


def recover_from_sell_balance_rejection(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    order_request: dict[str, Any],
    reason: str,
    mode: str,
    details: str,
) -> None:
    placeable = _parse_placeable_from_balance_error(details)
    live_balance = get_live_conditional_balance(context, order_request["token_id"])
    if live_balance is not None:
        sync_inventory_balance(market, inventory, order_request["token_id"], live_balance)
    context.analytics.log_event(
        "unwind_balance_rejected",
        {
            "market": market.slug,
            "reason": reason,
            "mode": mode,
            "order_request": order_request,
            "live_balance": live_balance,
            "placeable_after_matched": placeable,
            "details": details,
        },
    )
    print(
        f"[{market.slug}] {order_request['label']} rejected for insufficient balance/allowance; "
        f"live_balance={live_balance if live_balance is not None else 'unknown'} "
        f"placeable={placeable if placeable is not None else 'unknown'}"
    )
    if placeable is None or placeable + 1e-9 < EXCHANGE_MIN_ORDER_SIZE:
        inventory.balance_retry_pending = True
        inventory.balance_retry_deadline = time.time() + 30.0
        return
    inventory.balance_retry_pending = False
    inventory.balance_retry_deadline = 0.0
    retry_request = dict(order_request)
    retry_request["size"] = placeable
    try:
        retry_response = context.client.place_limit_order(
            token_id=retry_request["token_id"],
            side=retry_request["side"],
            price=retry_request["price"],
            size=retry_request["size"],
            tick_size=retry_request["tick_size"],
            dry_run=context.dry_run,
            neg_risk=market.neg_risk,
            post_only=not retry_request.get("ioc_escape", False),
            order_type=retry_request.get("order_type", "GTC"),
        )
        context.analytics.log_event(
            "unwind_balance_retry",
            {
                "market": market.slug,
                "size": placeable,
                "order_request": retry_request,
                "response": retry_response,
            },
        )
        print(f"[{market.slug}] {order_request['label']} retried with placeable size={placeable:.4f}")
        if response_is_success(retry_response):
            record_order_placement_event(
                context,
                retry_response,
                market=market,
                side=retry_request["side"],
                outcome="YES" if retry_request["token_id"] == market.yes_token_id else "NO",
                price=retry_request["price"],
                size=retry_request["size"],
                reward_eligible=False,
                reward_rate_per_day=market.reward_rate_per_day,
                fair_value=None,
                placement_mark=None,
            )
        log_rejected_order_response(
            context,
            market,
            f"{retry_request['label']} retry",
            retry_response,
            kind="unwind",
            mode=mode,
            reason=f"{reason}:balance_retry",
        )
    except Exception as retry_exc:
        context.analytics.log_event(
            "unwind_balance_retry_failed",
            {
                "market": market.slug,
                "size": placeable,
                "details": str(retry_exc),
            },
        )


def _parse_placeable_from_balance_error(details: str) -> Optional[float]:
    match = BALANCE_RE.search(details or "")
    if not match:
        return None
    try:
        balance_units = int(match.group(1))
        matched_units = int(match.group(2))
        placeable_units = max(balance_units - matched_units, 0)
        return placeable_units / 1_000_000.0
    except Exception:
        return None


def get_market_book(context: BotContext, token_id: str) -> Optional[OrderBookSnapshot]:
    try:
        return context.client.get_orderbook(token_id)
    except Exception as exc:
        context.analytics.log_event("orderbook_fetch_failed", {"token_id": token_id, "details": str(exc)})
        return None


def derive_sell_price(
    book: OrderBookSnapshot,
    fair_value: float,
    half_spread: float,
    aggressive: bool,
    ioc_escape: bool = False,
) -> float:
    if ioc_escape and book.best_bid is not None:
        escaped = max(book.best_bid - max(book.tick_size, 0.01), 0.01)
        return clamp_probability(round_to_tick(escaped, book.tick_size, down=True))
    if aggressive and book.best_bid is not None:
        return clamp_probability(round_sell_at_best_bid(book.best_bid, book.tick_size))
    target = clamp_probability(fair_value + half_spread)
    price = round_to_tick(target, book.tick_size, down=False)
    if book.best_bid is not None:
        min_post_only = round_to_tick(book.best_bid + max(book.tick_size, 0.01), book.tick_size, down=False)
        price = max(price, min_post_only)
    return clamp_probability(price)


def refresh_sell_unwind_order(context: BotContext, order_request: dict[str, Any], aggressive: bool) -> None:
    book = get_market_book(context, order_request["token_id"])
    if book is None:
        return
    order_request["tick_size"] = book.tick_size
    if order_request.get("ioc_escape"):
        order_request["price"] = derive_sell_price(book, 0.5, 0.0, aggressive=True, ioc_escape=True)
        return
    if aggressive and book.best_bid is not None:
        order_request["price"] = clamp_probability(round_sell_at_best_bid(book.best_bid, book.tick_size))


def retry_unwind_with_safe_price(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    order_request: dict[str, Any],
    reason: str,
    mode: str,
    details: str,
) -> Optional[Any]:
    book = get_market_book(context, order_request["token_id"])
    if book is None:
        return None
    safe_price = derive_safe_post_only_sell_price(book)
    if safe_price is None:
        return None
    order_request["price"] = safe_price
    order_request["tick_size"] = book.tick_size
    try:
        return context.client.place_limit_order(
            token_id=order_request["token_id"],
            side=order_request["side"],
            price=order_request["price"],
            size=order_request["size"],
            tick_size=order_request["tick_size"],
            dry_run=context.dry_run,
            neg_risk=market.neg_risk,
            post_only=True,
        )
    except Exception as retry_exc:
        context.analytics.log_event(
            "unwind_retry_error",
            {
                "market": market.slug,
                "reason": reason,
                "mode": mode,
                "order_request": order_request,
                "details": details,
                "retry_details": str(retry_exc),
            },
        )
        return None


def response_is_success(response: Any) -> bool:
    if not isinstance(response, dict):
        return False
    if bool(response.get("success")):
        return True
    status = str(response.get("status") or "").lower()
    return status in {"matched", "live", "filled"}


def derive_safe_post_only_sell_price(book: OrderBookSnapshot) -> Optional[float]:
    tick = max(book.tick_size, 0.01)
    if book.best_bid is not None:
        return clamp_probability(round_to_tick(book.best_bid + tick, book.tick_size, down=False))
    if book.best_ask is not None:
        return clamp_probability(round_to_tick(book.best_ask, book.tick_size, down=False))
    return None


def should_trim_position(reason: str) -> bool:
    return reason in {
        "yes inventory limit breached",
        "no inventory limit breached",
        "delta neutrality limit breached",
    }


def should_flatten_position(reason: str) -> bool:
    return reason in {
        "drawdown limit breached",
        "market is too close to resolution",
        "portfolio exposure limit breached",
    }


def print_quote_status(market: MarketConfig, book: OrderBookSnapshot, fair_value: float, bid: float, ask: float, net_delta: float) -> None:
    no_bid = round_no_bid_price(ask, book.tick_size)
    print(
        f"[{market.slug}] fair={fair_value:.3f} "
        f"top=({book.best_bid},{book.best_ask}) yes_bid={bid:.3f} yes_ask={ask:.3f} no_bid={no_bid:.3f} delta={net_delta:.1f}"
    )


def print_order_response(label: str, response: Any) -> None:
    order_id, status, reason = extract_order_response_metadata(response)
    if order_id or status or reason:
        reason_text = f" reason={reason}" if reason and reason != status else ""
        print(f"  {label}: status={status or '-'} order_id={order_id or '-'}{reason_text}")
        return
    print(f"  {label}: response={response}")


def extract_order_response_metadata(response: Any) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(response, dict):
        return None, None, None
    order_id = response.get("orderID") or response.get("order_id") or response.get("id")
    status_value = response.get("status")
    success_value = response.get("success")
    if status_value in (None, "") and isinstance(success_value, bool):
        status_value = "success" if success_value else "failed"
    elif status_value in (None, "") and success_value not in (None, ""):
        status_value = success_value
    reason = (
        response.get("errorMsg")
        or response.get("error")
        or response.get("error_message")
        or response.get("message")
        or response.get("reason")
        or response.get("rejectReason")
        or response.get("details")
    )
    status = str(status_value) if status_value not in (None, "") else None
    reason_text = str(reason) if reason not in (None, "") else None
    return order_id, status, reason_text


def is_rejected_order_response(response: Any) -> bool:
    if not isinstance(response, dict):
        return False
    if response.get("success") is False:
        return True
    _, status, reason = extract_order_response_metadata(response)
    if status is None:
        return reason is not None
    return status.strip().lower() not in {"dry_run", "live", "matched", "pending", "ok", "success"}


def log_rejected_order_response(
    context: BotContext,
    market: MarketConfig,
    label: str,
    response: Any,
    **details: Any,
) -> None:
    if not is_rejected_order_response(response):
        return
    _, status, reason = extract_order_response_metadata(response)
    payload = {
        "market": market.slug,
        "label": label,
        "status": status,
        "reason": reason,
        "response": response,
    }
    payload.update(details)
    context.analytics.log_event("order_rejected", payload)
    print(f"[{market.slug}] {label} rejected: status={status or 'unknown'} reason={reason or 'n/a'}")


def print_open_orders(context: BotContext, market: MarketConfig) -> None:
    try:
        yes_orders, no_orders = fetch_market_open_orders(context, market)
    except Exception as exc:
        print(f"  open orders check failed: {exc}")
        return
    print_open_orders_from_lists(yes_orders, no_orders)


def print_open_orders_from_lists(yes_orders: list[dict[str, Any]], no_orders: list[dict[str, Any]]) -> None:
    orders = yes_orders + no_orders
    print(f"  open_orders={len(orders)}")
    for order in orders[:6]:
        print(
            "   "
            f"id={short_id(order.get('id'))} side={order.get('side')} outcome={order.get('outcome')} "
            f"price={order.get('price')} size={order.get('original_size') or order.get('size')}"
        )


def fetch_market_open_orders(context: BotContext, market: MarketConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    yes_orders = normalize_open_orders(context.client.get_open_orders(asset_id=market.yes_token_id))
    no_orders = normalize_open_orders(context.client.get_open_orders(asset_id=market.no_token_id))
    return yes_orders, no_orders


def should_keep_existing_quotes(
    orders: list[dict[str, Any]],
    target_price: float,
    target_size: float,
    price_tolerance: float = 0.01,
    size_tolerance: float = 0.1,
) -> bool:
    for order in orders:
        side = str(order.get("side") or "").upper()
        if side != "BUY":
            continue
        order_price = _to_optional_float(order.get("price"))
        order_size = _to_optional_float(order.get("original_size") or order.get("size") or order.get("remaining_size"))
        if order_price is None or order_size is None:
            continue
        if abs(order_price - target_price) <= price_tolerance and abs(order_size - target_size) <= size_tolerance:
            return True
    return False


def cancel_market_orders_for_requote(
    context: BotContext,
    market: MarketConfig,
    yes_orders: list[dict[str, Any]],
    no_orders: list[dict[str, Any]],
) -> bool:
    if context.dry_run:
        return True
    if not yes_orders and not no_orders:
        return True
    try:
        if yes_orders:
            context.client.cancel_market_orders(market.yes_token_id, dry_run=False)
            for order in yes_orders:
                context.analytics.record_order_cancel(
                    str(order.get("id") or order.get("orderId") or ""),
                    market.slug,
                    side=str(order.get("side") or "BUY"),
                    outcome="YES",
                    price=_to_optional_float(order.get("price")),
                    size=_to_optional_float(order.get("original_size") or order.get("size") or order.get("remaining_size")),
                )
                context.analytics.note_cancel(market.slug)
        if no_orders:
            context.client.cancel_market_orders(market.no_token_id, dry_run=False)
            for order in no_orders:
                context.analytics.record_order_cancel(
                    str(order.get("id") or order.get("orderId") or ""),
                    market.slug,
                    side=str(order.get("side") or "BUY"),
                    outcome="NO",
                    price=_to_optional_float(order.get("price")),
                    size=_to_optional_float(order.get("original_size") or order.get("size") or order.get("remaining_size")),
                )
                context.analytics.note_cancel(market.slug)
        context.analytics.log_event(
            "cancel_market_requote",
            {
                "market": market.slug,
                "yes_orders": len(yes_orders),
                "no_orders": len(no_orders),
            },
        )
        print(f"[{market.slug}] canceled stale orders yes={len(yes_orders)} no={len(no_orders)}")
        return True
    except Exception as exc:
        context.analytics.log_event(
            "cancel_market_requote_error",
            {"market": market.slug, "error": str(exc)},
        )
        print(f"[{market.slug}] stale order cancel failed: {exc}")
        return False


def handle_telegram_commands(context: BotContext) -> None:
    commands = context.telegram.poll_commands()
    for command in commands:
        context.analytics.log_event("telegram_command", {"command": command})
        if command == "help":
            context.telegram.send_message(
                "Commands\n"
                "/start resume trading\n"
                "/stop pause trading and cancel live orders\n"
                "/status show current bot status"
            )
            continue
        if command == "status":
            context.telegram.send_message(build_status_message(context, prefix="Bot status"))
            continue
        if command == "start":
            already_running = context.control.trading_enabled
            context.control.enable("telegram:/start")
            prefix = "Trading already running" if already_running else "Trading resumed"
            context.telegram.send_message(build_status_message(context, prefix=prefix))
            continue
        if command == "stop":
            already_stopped = not context.control.trading_enabled
            context.control.disable("telegram:/stop")
            cancel_result = "dry_run_no_cancel" if context.dry_run else "cancel_all_ok"
            if not context.dry_run:
                try:
                    context.client.cancel_all_orders(dry_run=False)
                except Exception as exc:
                    cancel_result = f"cancel_all_failed: {exc}"
                    context.analytics.log_event("telegram_stop_cancel_error", {"error": str(exc)})
            prefix = "Trading already paused" if already_stopped else "Trading paused"
            context.telegram.send_message(build_status_message(context, prefix=f"{prefix}\n{cancel_result}"))


def build_status_message(context: BotContext, prefix: str) -> str:
    active_market = context.active_market.slug if context.active_market else "none"
    active_markets = ", ".join(market.slug for market in context.active_markets) if context.active_markets else "none"
    unwind_markets = ", ".join(market.slug for market in context.positions_to_unwind) if context.positions_to_unwind else "none"
    inventory_lines: list[str] = []
    for market_key, market_inventory in sorted(context.inventory.markets.items()):
        if market_inventory.gross_shares <= 0:
            continue
        inventory_lines.append(
            f"{market_key}: YES={market_inventory.yes_shares:.4f} "
            f"NO={market_inventory.no_shares:.4f} delta={market_inventory.net_delta:.4f}"
        )
    inventory_text = "\n".join(inventory_lines[:5]) if inventory_lines else "flat"
    return (
        f"{prefix}\n"
        f"trading_enabled={context.control.trading_enabled}\n"
        f"dry_run={context.dry_run}\n"
        f"force_unwind_only={context.force_unwind_only}\n"
        f"active_market={active_market}\n"
        f"active_markets={active_markets}\n"
        f"positions_to_unwind={unwind_markets}\n"
        f"loop_interval_seconds={context.loop_interval_seconds}\n"
        f"last_control_source={context.control.state.source}\n"
        f"inventory={inventory_text}"
    )


def normalize_open_orders(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        data = response.get("data") or response.get("orders") or response.get("results")
        if isinstance(data, list):
            return data
    return []


def short_id(value: Any) -> str:
    text = str(value or "")
    if len(text) <= 14:
        return text
    return f"{text[:8]}...{text[-6:]}"


def passes_post_only_guard(book: OrderBookSnapshot, bid: float, ask: float) -> bool:
    if bid >= ask:
        return False
    if book.best_ask is not None and bid >= book.best_ask:
        return False
    if book.best_bid is not None and ask <= book.best_bid:
        return False
    return True


def adjust_quote_to_post_only(book: OrderBookSnapshot, quote: Any) -> bool:
    tick = max(book.tick_size, 0.01)
    adjusted = False

    if book.best_ask is not None and quote.bid >= book.best_ask:
        quote.bid = round_to_tick(clamp_probability(book.best_ask - tick), book.tick_size, down=True)
        adjusted = True

    if book.best_bid is not None and quote.ask <= book.best_bid:
        quote.ask = round_to_tick(clamp_probability(book.best_bid + tick), book.tick_size, down=False)
        adjusted = True

    if quote.ask <= quote.bid:
        quote.ask = round_to_tick(clamp_probability(quote.bid + tick), book.tick_size, down=False)
        adjusted = True

    if book.best_ask is not None and quote.bid >= book.best_ask:
        safe_bid = clamp_probability(book.best_ask - tick)
        if safe_bid < quote.ask:
            quote.bid = round_to_tick(safe_bid, book.tick_size, down=True)
            adjusted = True

    if book.best_bid is not None and quote.ask <= book.best_bid:
        safe_ask = clamp_probability(book.best_bid + tick)
        if safe_ask > quote.bid:
            quote.ask = round_to_tick(safe_ask, book.tick_size, down=False)
            adjusted = True

    return adjusted


def is_trading_restricted_error(exc: Exception) -> bool:
    return "Trading restricted in your region" in str(exc)


def is_insufficient_balance_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "not enough balance / allowance" in message or "balance is not enough" in message


def is_cancel_only_error(exc: Exception) -> bool:
    return "cancel-only" in str(exc).lower()


def pause_for_cancel_only(context: BotContext, market_slug: str, exc: Exception) -> None:
    context.exchange_pause_until = max(context.exchange_pause_until, time.time() + 300.0)
    context.exchange_pause_reason = "polymarket:cancel_only"
    context.analytics.log_event(
        "exchange_cancel_only_pause",
        {
            "market": market_slug,
            "pause_until": context.exchange_pause_until,
            "details": str(exc),
        },
    )
    if context.telegram.is_enabled():
        context.telegram.send_message(
            "Exchange entered cancel-only mode\n"
            f"market={market_slug}\n"
            "trading paused for 5 minutes"
        )


def is_crosses_book_error(exc: Exception) -> bool:
    return "crosses the book" in str(exc).lower()


def round_no_bid_price(yes_ask: float, tick_size: float) -> float:
    tick = max(tick_size, 0.01)
    raw = max(0.01, min(0.99, 1.0 - yes_ask))
    return round(int(raw / tick) * tick, 4)


def determine_quote_sides(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    signal: Any,
    book: OrderBookSnapshot,
) -> tuple[bool, bool, str]:
    yes_limit = max(
        context.risk.min_order_size,
        context.risk.max_yes_shares_per_market * context.risk.requote_inventory_fraction_limit,
    )
    no_limit = max(
        context.risk.min_order_size,
        context.risk.max_no_shares_per_market * context.risk.requote_inventory_fraction_limit,
    )
    now = time.time()
    effective_cooldown = max(context.fill_cooldown_seconds, inventory.fill_cooldown_seconds_override)
    if (
        effective_cooldown > 0
        and inventory.last_fill_ts > 0
        and now - inventory.last_fill_ts < effective_cooldown
    ):
        remaining = int(max(effective_cooldown - (now - inventory.last_fill_ts), 0))
        if inventory.net_delta > 0.01:
            return False, True, f"post_fill_cooldown_rebalance_yes({remaining}s)"
        if inventory.net_delta < -0.01:
            return True, False, f"post_fill_cooldown_rebalance_no({remaining}s)"
        return False, False, f"post_fill_cooldown_flat({remaining}s)"

    place_yes_bid = inventory.yes_shares < yes_limit
    place_no_bid = inventory.no_shares < no_limit
    toxic_yes, toxic_no, toxic_reason = toxic_flow_quote_filter(context, market, inventory, signal, book)
    if toxic_yes:
        place_yes_bid = False
    if toxic_no:
        place_no_bid = False

    if place_yes_bid and place_no_bid:
        return True, True, "normal_quote"
    if place_yes_bid and not place_no_bid:
        if toxic_reason:
            return True, False, toxic_reason
        return True, False, "inventory_rebalance_only_no_side_full"
    if place_no_bid and not place_yes_bid:
        if toxic_reason:
            return False, True, toxic_reason
        return False, True, "inventory_rebalance_only_yes_side_full"
    if toxic_reason:
        return False, False, toxic_reason
    return False, False, "inventory_limits_block_both_sides"


def derive_quote_size(context: BotContext, market: MarketConfig, base_size: float) -> float:
    configured_size = max(base_size, context.risk.min_order_size)
    # In live mode we must respect the user's configured clip size. Reward
    # minimums inform market selection, but they should not silently upsize
    # orders beyond the configured exposure budget.
    max_safe_size = max(
        context.risk.min_order_size,
        min(
            context.risk.max_yes_shares_per_market,
            context.risk.max_no_shares_per_market,
            max(context.risk.max_net_delta_per_market, context.risk.min_order_size),
        ),
    )
    return min(configured_size, max_safe_size)


def configured_quote_capacity(context: BotContext) -> float:
    return min(
        max(context.pricing.size_per_order, context.risk.min_order_size),
        max(
            context.risk.min_order_size,
            min(
                context.risk.max_yes_shares_per_market,
                context.risk.max_no_shares_per_market,
                max(context.risk.max_net_delta_per_market, context.risk.min_order_size),
            ),
        ),
    )


def normalize_reward_spread_limit(value: Any) -> Optional[float]:
    spread = _to_optional_float(value)
    if spread is None or spread <= 0:
        return None
    return spread / 100.0 if spread > 1.0 else spread


def apply_reward_quote_constraints(market: MarketConfig, quote: Quote, book: OrderBookSnapshot) -> None:
    reward_max_spread = normalize_reward_spread_limit(market.rewards_max_spread)
    if reward_max_spread is None:
        return
    if quote.half_spread <= reward_max_spread:
        return
    quote.half_spread = reward_max_spread
    tick = max(book.tick_size, 0.01)
    capped_bid = clamp_probability(quote.fair_value - reward_max_spread)
    capped_ask = clamp_probability(quote.fair_value + reward_max_spread)
    quote.bid = round_to_tick(max(quote.bid, capped_bid), book.tick_size, down=True)
    quote.ask = round_to_tick(min(quote.ask, capped_ask), book.tick_size, down=False)
    if book.best_bid is not None:
        quote.ask = max(quote.ask, clamp_probability(book.best_bid + tick))
    if quote.ask <= quote.bid:
        quote.ask = clamp_probability(quote.bid + tick)


def evaluate_reward_eligibility(market: MarketConfig, quote: Quote, place_both_sides: bool) -> dict[str, Any]:
    reward_rate = _to_optional_float(market.reward_rate_per_day) or 0.0
    if reward_rate <= 0:
        return {"eligible": False, "reason": "not_reward_market"}
    reward_min_size = _to_optional_float(market.rewards_min_size)
    if reward_min_size is not None and quote.size + 1e-9 < reward_min_size:
        return {"eligible": False, "reason": f"size_below_reward_min ({quote.size:.2f} < {reward_min_size:.2f})"}
    reward_max_spread = normalize_reward_spread_limit(market.rewards_max_spread)
    if reward_max_spread is not None and quote.half_spread - reward_max_spread > 1e-9:
        return {
            "eligible": False,
            "reason": f"spread_above_reward_max ({quote.half_spread:.4f} > {reward_max_spread:.4f})",
        }
    if (quote.fair_value < 0.10 or quote.fair_value > 0.90) and not place_both_sides:
        return {"eligible": False, "reason": "extreme_midpoint_requires_two_sided_quotes"}
    return {"eligible": True, "reason": "eligible"}


def current_drawdown_ratio(context: BotContext, inventory: Any) -> float:
    limit = max(float(context.risk.max_drawdown_usdc), 0.0)
    if limit <= 0:
        return 0.0
    return max(-float(inventory.realized_pnl), 0.0) / limit


def effective_pricing_config(context: BotContext, drawdown_ratio: float) -> PricingConfig:
    if drawdown_ratio < 0.50:
        return context.pricing
    return replace(
        context.pricing,
        min_half_spread=min(context.pricing.max_half_spread, context.pricing.min_half_spread * 2.0),
        size_per_order=max(context.risk.min_order_size, context.pricing.size_per_order / 2.0),
    )


def quote_safe_mark(signal: Any) -> float:
    if signal.midpoint is not None:
        return signal.midpoint
    if signal.last_trade is not None:
        return signal.last_trade
    return 0.5


def apply_adaptive_fill_cooldown(context: BotContext, inventory: Any, signal: Any) -> None:
    current_mark = quote_safe_mark(signal)
    if inventory.last_fill_ts <= 0 or inventory.last_mark_price <= 0:
        inventory.fill_cooldown_seconds_override = 0
        return
    adverse_move = adverse_move_magnitude(inventory, current_mark)
    vol_unit = max(signal.realized_volatility, 0.01)
    if adverse_move >= 1.5 * vol_unit:
        inventory.fill_cooldown_seconds_override = max(context.fill_cooldown_seconds, ADAPTIVE_FILL_COOLDOWN_STRONG)
        return
    if adverse_move >= 0.75 * vol_unit:
        inventory.fill_cooldown_seconds_override = max(context.fill_cooldown_seconds, ADAPTIVE_FILL_COOLDOWN_MILD)
        return
    inventory.fill_cooldown_seconds_override = 0


def adverse_move_magnitude(inventory: Any, current_mark: float) -> float:
    if inventory.last_mark_price <= 0:
        return 0.0
    move = current_mark - inventory.last_mark_price
    if inventory.net_delta > 0.01:
        return max(-move, 0.0)
    if inventory.net_delta < -0.01:
        return max(move, 0.0)
    return 0.0


def toxic_flow_quote_filter(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    signal: Any,
    book: OrderBookSnapshot,
) -> tuple[bool, bool, str]:
    skip_yes = False
    skip_no = False
    reasons: list[str] = []
    trades = context.client.get_recent_trades(market.yes_token_id, limit=TOXIC_FLOW_TRADE_WINDOW)
    if trades:
        first_trade = trades[0] if isinstance(trades[0], dict) else {}
        context.analytics.log_event(
            "toxic_flow_trade_sample",
            {
                "market": market.slug,
                "count": len(trades),
                "first_keys": list(first_trade.keys()) if isinstance(first_trade, dict) else [],
                "first_side": first_trade.get("side") if isinstance(first_trade, dict) else None,
            },
        )
    if not trades and inventory.last_mark_price > 0:
        context.analytics.log_event("toxic_flow_no_trades", {"market": market.slug})
    buy_count = 0
    sell_count = 0
    for trade in trades:
        side = normalize_trade_flow_side(trade)
        if side == "BUY":
            buy_count += 1
        elif side == "SELL":
            sell_count += 1
    total = buy_count + sell_count
    if total > 0:
        buy_ratio = buy_count / total
        sell_ratio = sell_count / total
        if sell_ratio > TOXIC_FLOW_ONE_WAY_THRESHOLD:
            skip_yes = True
            reasons.append(f"toxic_flow_skip_yes_bid({sell_ratio:.0%}_sell)")
        if buy_ratio > TOXIC_FLOW_ONE_WAY_THRESHOLD:
            skip_no = True
            reasons.append(f"toxic_flow_skip_no_bid({buy_ratio:.0%}_buy)")
    elif book.bid_depth_usdc > 0 and book.ask_depth_usdc > 0:
        total_depth = book.bid_depth_usdc + book.ask_depth_usdc
        if total_depth > 0:
            imbalance = (book.bid_depth_usdc - book.ask_depth_usdc) / total_depth
            if imbalance < -0.60:
                skip_yes = True
                reasons.append(f"toxic_book_imbalance_asks({imbalance:.2f})")
            elif imbalance > 0.60:
                skip_no = True
                reasons.append(f"toxic_book_imbalance_bids({imbalance:.2f})")
    current_mark = quote_safe_mark(signal)
    if inventory.last_mark_price > 0:
        move = current_mark - inventory.last_mark_price
        threshold = max(signal.realized_volatility * TOXIC_FLOW_VOL_MULTIPLIER, max(book.tick_size, 0.01))
        if move <= -threshold:
            skip_yes = True
            reasons.append(f"toxic_mid_down({move:.3f})")
        if move >= threshold:
            skip_no = True
            reasons.append(f"toxic_mid_up({move:.3f})")
    context.analytics.emit_quote_cycle_flow(
        market.slug,
        recent_buy_count=buy_count,
        recent_sell_count=sell_count,
        buy_ratio=(buy_count / total) if total > 0 else None,
        mid_move_last_cycle=(current_mark - inventory.last_mark_price) if inventory.last_mark_price > 0 else 0.0,
        would_skip_yes=skip_yes,
        would_skip_no=skip_no,
    )
    return skip_yes, skip_no, " ".join(reasons)


def normalize_trade_flow_side(trade: dict[str, Any]) -> str:
    raw_side = str(trade.get("side") or trade.get("Side") or "").upper()
    if raw_side in {"BUY", "TAKER_BUY", "BUYER", "BID"}:
        return "BUY"
    if raw_side in {"SELL", "TAKER_SELL", "SELLER", "ASK"}:
        return "SELL"
    return raw_side


def should_trigger_unwind_ioc_escape(inventory: Any, signal: Any, aggressive: bool) -> bool:
    if aggressive and inventory.unwind_cycles_without_fill >= UNWIND_IOC_ESCAPE_CYCLES:
        return True
    adverse_move = adverse_move_magnitude(inventory, quote_safe_mark(signal))
    vol_unit = max(signal.realized_volatility, 0.01)
    return adverse_move >= UNWIND_IOC_ESCAPE_VOL_MULTIPLIER * vol_unit


def response_is_immediate_execution(response: Any) -> bool:
    if not isinstance(response, dict):
        return False
    status = str(response.get("status") or "").lower()
    return status in {"matched", "filled"}


def maybe_handle_dust_position(context: BotContext, market: MarketConfig, inventory: Any) -> bool:
    if is_non_actionable_dust_position(context, market, inventory):
        if market.slug not in context.released_markets:
            context.released_markets.add(market.slug)
            context.analytics.log_event(
                "dust_position_released",
                {
                    "market": market.slug,
                    "yes_shares": inventory.yes_shares,
                    "no_shares": inventory.no_shares,
                    "recoverable_yes_usdc": inventory.yes_shares * estimate_outcome_recovery_price(inventory, "YES"),
                    "recoverable_no_usdc": inventory.no_shares * estimate_outcome_recovery_price(inventory, "NO"),
                    "reason": "non_actionable_dust",
                },
            )
            print(
                f"[{market.slug}] released non-actionable dust "
                f"yes={inventory.yes_shares:.4f} no={inventory.no_shares:.4f}"
            )
        return True

    paired_dust = min(inventory.yes_shares, inventory.no_shares)
    if 0 < paired_dust < EXCHANGE_MIN_ORDER_SIZE:
        recoverable_usdc = paired_dust
        if recoverable_usdc <= MERGE_GAS_FALLBACK_USDC:
            status = "oof"
        elif not market.condition_id:
            status = "merge_missing_condition_id"
        else:
            try:
                response = context.client.merge_position_pair(market.condition_id, paired_dust, dry_run=context.dry_run)
                inventory.yes_shares = max(inventory.yes_shares - paired_dust, 0.0)
                inventory.no_shares = max(inventory.no_shares - paired_dust, 0.0)
                inventory.realized_pnl += paired_dust
                status = str(response.get("status") or "merge_submitted")
                context.analytics.log_event(
                    "dust_merge_executed",
                    {
                        "market": market.slug,
                        "condition_id": market.condition_id,
                        "paired_shares": paired_dust,
                        "recoverable_usdc": recoverable_usdc,
                        "response": response,
                    },
                )
                print(
                    f"[{market.slug}] dust merged: paired={paired_dust:.4f} "
                    f"recoverable_usdc={recoverable_usdc:.4f} tx={response.get('tx_hash', '-')}"
                )
                context.analytics.emit_merge_pnl_impact(
                    market.slug,
                    paired_dust,
                    usdc_received=paired_dust,
                    gas_used=_to_optional_float(response.get("gas_used")),
                )
                if not context.dry_run:
                    run_recovery_reconciliation(context, "dust_merge", market=market.slug, shares=paired_dust)
                inventory.unwind_failed_cycles = 0
                return True
            except Exception as exc:
                status = "merge_failed"
                context.analytics.log_event(
                    "dust_merge_error",
                    {
                        "market": market.slug,
                        "condition_id": market.condition_id,
                        "paired_shares": paired_dust,
                        "recoverable_usdc": recoverable_usdc,
                        "details": str(exc),
                    },
                )
                print(f"[{market.slug}] dust merge failed: {exc}")
        context.analytics.log_event(
            "dust_merge_candidate",
            {
                "market": market.slug,
                "yes_shares": inventory.yes_shares,
                "no_shares": inventory.no_shares,
                "paired_shares": paired_dust,
                "recoverable_usdc": recoverable_usdc,
                "status": status,
            },
        )
        print(
            f"[{market.slug}] dust handling: paired_dust={paired_dust:.4f} "
            f"recoverable_usdc={recoverable_usdc:.4f} status={status}"
        )
        return False
    if 0 < inventory.yes_shares < EXCHANGE_MIN_ORDER_SIZE or 0 < inventory.no_shares < EXCHANGE_MIN_ORDER_SIZE:
        context.analytics.log_event(
            "dust_residual_position",
            {
                "market": market.slug,
                "yes_shares": inventory.yes_shares,
                "no_shares": inventory.no_shares,
            },
        )
        return False
    return False


def release_stuck_unwind_market(context: BotContext, market: MarketConfig, inventory: Any) -> None:
    context.released_markets.add(market.slug)
    inventory.unwind_failed_cycles = 0
    context.analytics.log_event(
        "stuck_unwind_release",
        {
            "market": market.slug,
            "yes_shares": inventory.yes_shares,
            "no_shares": inventory.no_shares,
            "realized_loss": inventory.realized_pnl,
        },
    )
    if context.telegram.is_enabled():
        context.telegram.send_message(
            f"Stuck unwind released: {market.slug}\n"
            f"yes={inventory.yes_shares:.4f} no={inventory.no_shares:.4f}"
        )
    print(
        f"[{market.slug}] released from stuck unwind "
        f"yes={inventory.yes_shares:.4f} no={inventory.no_shares:.4f}"
    )


def attempt_merge_before_flatten(context: BotContext, market: MarketConfig, inventory: Any, reason: str) -> Optional[str]:
    paired_shares = min(inventory.yes_shares, inventory.no_shares)
    if paired_shares <= 0.01:
        return None
    if not market.condition_id:
        context.analytics.log_event(
            "merge_skip",
            {"market": market.slug, "reason": "missing_condition_id", "paired_shares": paired_shares},
        )
        return None
    try:
        response = context.client.merge_position_pair(market.condition_id, paired_shares, dry_run=context.dry_run)
    except Exception as exc:
        context.analytics.log_event(
            "merge_error",
            {
                "market": market.slug,
                "reason": reason,
                "paired_shares": paired_shares,
                "details": str(exc),
            },
        )
        print(f"[{market.slug}] merge before flatten failed: {exc}")
        return None
    inventory.yes_shares = max(inventory.yes_shares - paired_shares, 0.0)
    inventory.no_shares = max(inventory.no_shares - paired_shares, 0.0)
    inventory.realized_pnl += paired_shares
    context.analytics.log_event(
        "flatten_pair_merge",
        {
            "market": market.slug,
            "reason": reason,
            "paired_shares": paired_shares,
            "response": response,
        },
    )
    print(
        f"[{market.slug}] merged paired YES/NO before flatten "
        f"paired={paired_shares:.4f} status={response.get('status', 'submitted')}"
    )
    context.analytics.emit_merge_pnl_impact(
        market.slug,
        paired_shares,
        usdc_received=paired_shares,
        gas_used=_to_optional_float(response.get("gas_used")),
    )
    if not context.dry_run:
        run_recovery_reconciliation(context, "flatten_pair_merge", market=market.slug, shares=paired_shares)
    if inventory.gross_shares <= 0.01:
        return "merged_all"
    return "merged_partial"


def record_order_placement_event(
    context: BotContext,
    response: Any,
    *,
    market: MarketConfig,
    side: str,
    outcome: str,
    price: float,
    size: float,
    reward_eligible: bool,
    reward_rate_per_day: Optional[float],
    fair_value: Optional[float],
    placement_mark: Optional[float] = None,
) -> None:
    order_id, status, _ = extract_order_response_metadata(response)
    if not order_id:
        return
    if status and status.strip().lower() in {"rejected", "error"}:
        return
    context.analytics.record_order_placement(
        order_id=order_id,
        market=market.slug,
        side=side,
        outcome=outcome,
        price=price,
        size=size,
        reward_eligible=reward_eligible,
        reward_rate_per_day=reward_rate_per_day,
        fair_value=fair_value,
        placement_mark=placement_mark,
    )


def round_sell_at_best_bid(best_bid: float, tick_size: float) -> float:
    down_price = round_to_tick(best_bid, tick_size, down=True)
    if best_bid - down_price > 1e-9:
        up_price = round_to_tick(best_bid, tick_size, down=False)
        if up_price >= best_bid - 1e-9:
            return up_price
    return down_price


def is_non_actionable_dust_position(context: BotContext, market: MarketConfig, inventory: Any) -> bool:
    if inventory.gross_shares <= 0.01:
        return False
    max_side = max(inventory.yes_shares, inventory.no_shares)
    if max_side + 1e-9 >= EXCHANGE_MIN_ORDER_SIZE:
        return False
    yes_mark = estimate_outcome_recovery_price(inventory, "YES")
    no_mark = estimate_outcome_recovery_price(inventory, "NO")
    unpaired_recovery = max(inventory.yes_shares * yes_mark, inventory.no_shares * no_mark)
    paired_dust = min(inventory.yes_shares, inventory.no_shares)
    if paired_dust > 0.0:
        recoverable_usdc = paired_dust
        if recoverable_usdc > MERGE_GAS_FALLBACK_USDC and market.condition_id:
            return False
        return unpaired_recovery <= MERGE_GAS_FALLBACK_USDC
    return unpaired_recovery <= MERGE_GAS_FALLBACK_USDC


def estimate_outcome_recovery_price(inventory: Any, outcome: str) -> float:
    base_mark = float(inventory.last_mark_price) if float(getattr(inventory, "last_mark_price", 0.0)) > 0 else 0.5
    base_mark = min(max(base_mark, 0.01), 0.99)
    if outcome.upper() == "YES":
        return base_mark
    return min(max(1.0 - base_mark, 0.01), 0.99)


def inventory_requires_active_unwind(context: BotContext, market: MarketConfig, inventory: Any) -> bool:
    if inventory.gross_shares <= 0.01:
        return False
    return not is_non_actionable_dust_position(context, market, inventory)


def reward_spread_limit_breach(market: MarketConfig, quote: Quote) -> Optional[str]:
    reward_max_spread = normalize_reward_spread_limit(market.rewards_max_spread)
    if reward_max_spread is None:
        return None
    if quote.half_spread - reward_max_spread > 1e-9:
        return f"reward spread cap breached ({quote.half_spread:.4f} > {reward_max_spread:.4f})"
    return None


def validate_market_config(markets: list[MarketConfig]) -> None:
    if not markets:
        raise RuntimeError("No markets configured in config.yaml")
    placeholders = []
    for market in markets:
        if market.yes_token_id == "YES_TOKEN_ID" or market.no_token_id == "NO_TOKEN_ID":
            placeholders.append(market.slug)
    if placeholders:
        joined = ", ".join(placeholders)
        raise RuntimeError(
            f"Placeholder token IDs found for configured markets: {joined}. "
            "Replace YES_TOKEN_ID/NO_TOKEN_ID in config.yaml with real Polymarket token IDs first."
        )


def initialize_market_selection(context: BotContext) -> None:
    manual_markets = [market for market in context.markets if not is_placeholder_market(market)]
    if context.force_unwind_only:
        context.markets = manual_markets
        context.active_markets = []
        context.active_market = None
        return
    if not context.auto_scan_enabled and not manual_markets:
        raise RuntimeError("Auto-scan is disabled and no valid manual markets are configured in config.yaml")
    if manual_markets and not context.auto_scan_enabled:
        validate_market_config(manual_markets)
        context.markets = manual_markets
        context.positions_to_unwind = [
            market for market in manual_markets if context.inventory.get(market.slug).gross_shares > 0.01
        ]
        context.active_markets = manual_markets[: context.max_active_markets]
        context.active_market = context.active_markets[0] if context.active_markets else None
        return
    context.markets = manual_markets
    refresh_market_selection(context, force=True)


def refresh_market_selection(context: BotContext, force: bool = False) -> None:
    prune_reentry_cooldowns(context)
    context.active_markets = [
        market
        for market in context.active_markets
        if inventory_requires_active_unwind(context, market, context.inventory.get(market.slug))
        or context.inventory.get(market.slug).gross_shares <= 0.01
    ]
    context.active_market = context.active_markets[0] if context.active_markets else None
    if context.force_unwind_only:
        context.positions_to_unwind = [
            market
            for market in context.positions_to_unwind
            if inventory_requires_active_unwind(context, market, context.inventory.get(market.slug))
        ]
        context.active_markets = context.positions_to_unwind[: context.max_active_markets]
        context.active_market = context.active_markets[0] if context.active_markets else None
        return
    if not context.auto_scan_enabled:
        return
    context.positions_to_unwind = [
        market
        for market in context.positions_to_unwind
        if inventory_requires_active_unwind(context, market, context.inventory.get(market.slug))
    ]
    now = time.time()
    must_rescan = (
        force
        or not context.active_markets
        or len(context.active_markets) < context.max_active_markets
        or should_rescan_active_market(context)
    )
    if not must_rescan and now - context.last_scan_ts < context.rescan_interval_seconds:
        return
    selected = select_best_markets(context)
    context.last_scan_ts = now
    if selected is None:
        context.active_market = None
        context.active_markets = []
        context.analytics.log_event("selection_skip", {"reason": "no_candidate_found"})
        return
    previous_slugs = [market.slug for market in context.active_markets]
    new_slugs = [market.slug for market in selected]
    context.active_markets = selected
    context.active_market = selected[0] if selected else None
    if previous_slugs != new_slugs:
        context.analytics.log_event(
            "market_switch",
            {"previous_markets": previous_slugs, "new_markets": new_slugs},
        )
        for previous_slug in previous_slugs:
            previous_inventory = context.inventory.get(previous_slug)
            context.analytics.emit_market_pnl_snapshot(
                previous_slug,
                previous_inventory,
                previous_inventory.last_mark_price,
            )
        print("Selected markets:")
        for market in selected:
            print(f"  {market.slug} | {market.question}")


def get_markets_to_quote(context: BotContext) -> list[MarketConfig]:
    if context.force_unwind_only:
        return [market for market in context.positions_to_unwind if market.slug not in context.released_markets][
            : context.max_active_markets
        ]
    if context.auto_scan_enabled:
        return [market for market in context.active_markets if market.slug not in context.released_markets]
    return [market for market in context.markets if market.slug not in context.released_markets]


def preflight_live_auth(context: BotContext) -> None:
    try:
        geo = context.client.check_geoblock()
    except Exception as exc:
        raise RuntimeError("Live mode geoblock preflight failed. Could not verify trading eligibility.") from exc
    if geo.get("blocked"):
        raise RuntimeError(
            "Live trading is blocked from this detected region. "
            f"country={geo.get('country')} region={geo.get('region')}. "
            "Polymarket will reject order placement from restricted regions."
        )
    try:
        context.client.validate_auth()
    except Exception as exc:
        raise RuntimeError(
            "Live mode auth preflight failed before canceling or placing orders. "
            "Check that POLY_CLOB_API_KEY, POLY_CLOB_SECRET, and POLY_CLOB_PASS_PHRASE are CLOB trading credentials "
            "for the same wallet/funder in config.yaml. Relayer/builder keys will not work for CLOB order endpoints."
        ) from exc
    collateral = context.client.get_collateral_balance_allowance()
    balance = parse_usdc_amount(collateral.get("balance"))
    allowances = collateral.get("allowances") or {}
    max_allowance = max((parse_usdc_amount(value) for value in allowances.values()), default=parse_usdc_amount(collateral.get("allowance")))
    if balance <= 0 or max_allowance <= 0:
        raise RuntimeError(
            "Live mode balance preflight failed: CLOB reports zero collateral balance or allowance. "
            f"balance={collateral.get('balance')} allowances={allowances}. "
            "Check that config.yaml funder/signature_type match the funded Polymarket wallet and that USDC is approved for CLOB trading."
        )


def should_rescan_active_market(context: BotContext) -> bool:
    if not context.active_markets:
        return True
    for market in context.active_markets:
        if market.end_date is None:
            continue
        days_left = max((market.end_date - datetime.now(timezone.utc)).total_seconds() / 86400.0, 0.0)
        if days_left < context.risk.min_days_to_resolution and context.inventory.get(market.slug).gross_shares <= 0.01:
            return True
    return False


def select_best_markets(context: BotContext) -> Optional[list[MarketConfig]]:
    scan_source = "gamma"
    reward_reason_counts: dict[str, int] = {}
    gamma_reason_counts: dict[str, int] = {}
    live_reason_counts: dict[str, int] = {}
    filtered_candidates, raw_count, normalized_count, reward_reason_counts = scan_reward_candidates(context)
    if filtered_candidates:
        scan_source = "rewards"
    elif bool(context.filters.get("rewards_only", False)):
        scan_source = "rewards"
    else:
        filtered_candidates, raw_count, normalized_count, gamma_reason_counts = scan_gamma_candidates(context)
    if not filtered_candidates:
        fallback = merge_selected_markets(context, [])
        if fallback:
            print(f"Market scan found no candidates; keeping {len(fallback)} unwind market(s)")
            return fallback
        print(
            f"Market scan found no candidates: source={scan_source} raw={raw_count} normalized={normalized_count} "
            f"reasons={format_reason_counts(reward_reason_counts if scan_source == 'rewards' else gamma_reason_counts)}"
        )
        return None
    filtered_candidates.sort(key=prebook_candidate_priority, reverse=True)
    shortlisted = filtered_candidates[: context.scan_candidate_cap]
    candidates: list[tuple[float, MarketConfig]] = []
    checked_books = 0
    for normalized in shortlisted:
        try:
            book = context.client.get_orderbook(normalized["yes_token_id"])
        except Exception as exc:
            context.analytics.log_event("scan_book_error", {"market": normalized["slug"], "details": str(exc)})
            increment_reason(live_reason_counts, "orderbook_fetch_failed")
            continue
        checked_books += 1
        spread_bps = ((book.spread or 0.0) * 10000.0)
        if spread_bps < float(context.filters.get("min_spread_bps", 0.0)):
            increment_reason(live_reason_counts, "live_spread_too_small")
            continue
        min_book_depth_usdc = _to_optional_float(context.filters.get("min_book_depth_usdc"))
        if min_book_depth_usdc is not None:
            usable_depth = min(book.bid_depth_usdc, book.ask_depth_usdc)
            if usable_depth + 1e-9 < min_book_depth_usdc:
                increment_reason(live_reason_counts, "live_book_depth_too_shallow")
                continue
        live_reference_price = book.midpoint if book.midpoint is not None else normalized.get("reference_price")
        if fair_value_band_breach(live_reference_price, context.filters) is not None:
            increment_reason(live_reason_counts, "live_fair_value_out_of_band")
            continue
        neg_risk = normalized.get("neg_risk")
        if neg_risk is None:
            try:
                neg_risk = context.client.get_neg_risk(normalized["yes_token_id"])
            except Exception as exc:
                context.analytics.log_event("scan_neg_risk_error", {"market": normalized["slug"], "details": str(exc)})
                neg_risk = None
        market = MarketConfig(
            slug=normalized["slug"],
            question=normalized["question"],
            yes_token_id=normalized["yes_token_id"],
            no_token_id=normalized["no_token_id"],
            end_date=parse_end_date(normalized.get("end_date")),
            neg_risk=neg_risk,
            condition_id=normalized.get("condition_id"),
            reward_rate_per_day=_to_optional_float(normalized.get("reward_rate_per_day")),
            rewards_min_size=_to_optional_float(normalized.get("rewards_min_size")),
            rewards_max_spread=_to_optional_float(normalized.get("rewards_max_spread")),
            market_competitiveness=_to_optional_float(normalized.get("market_competitiveness")),
        )
        score = score_market_candidate(
            normalized["volume_24h"],
            spread_bps,
            reward_rate_per_day=_to_optional_float(normalized.get("reward_rate_per_day")),
            market_competitiveness=_to_optional_float(normalized.get("market_competitiveness")),
            reward_min_size=_to_optional_float(normalized.get("rewards_min_size")),
            days_to_resolution=days_to_resolution_from_value(normalized.get("end_date")),
            sports_preference=is_sports_market(normalized),
        )
        candidates.append((score, market))
    if not candidates:
        fallback = merge_selected_markets(context, [])
        if fallback:
            print(
                "Market scan found no live-book candidates; "
                f"keeping {len(fallback)} unwind market(s): source={scan_source} raw={raw_count} normalized={normalized_count} "
                f"filter_pass={len(filtered_candidates)} checked_books={checked_books} "
                f"filter_reasons={format_reason_counts(reward_reason_counts if scan_source == 'rewards' else gamma_reason_counts)} "
                f"live_reasons={format_reason_counts(live_reason_counts)}"
            )
            return fallback
        print(
            "Market scan found no live-book candidates: "
            f"source={scan_source} raw={raw_count} normalized={normalized_count} "
            f"filter_pass={len(filtered_candidates)} checked_books={checked_books} "
            f"filter_reasons={format_reason_counts(reward_reason_counts if scan_source == 'rewards' else gamma_reason_counts)} "
            f"live_reasons={format_reason_counts(live_reason_counts)}"
        )
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    selected = merge_selected_markets(context, [market for _, market in candidates])
    print(
        "Market scan selected "
        f"{len(selected)} markets: source={scan_source} raw={raw_count} normalized={normalized_count} "
        f"filter_pass={len(filtered_candidates)} checked_books={checked_books} "
        f"filter_reasons={format_reason_counts(reward_reason_counts if scan_source == 'rewards' else gamma_reason_counts)} "
        f"live_reasons={format_reason_counts(live_reason_counts)}"
    )
    return selected


def scan_gamma_candidates(context: BotContext) -> tuple[list[dict[str, Any]], int, int, dict[str, int]]:
    filtered_candidates: list[dict[str, Any]] = []
    cache_candidates: list[MarketConfig] = []
    raw_count = 0
    normalized_count = 0
    reason_counts: dict[str, int] = {}
    for page in range(context.scan_pages):
        try:
            raw_markets = context.client.scan_markets(limit=context.scan_limit, offset=page * context.scan_limit)
        except Exception as exc:
            context.analytics.log_event("scan_error", {"page": page, "details": str(exc), "source": "gamma"})
            increment_reason(reason_counts, "gamma_scan_error")
            continue
        for item in raw_markets:
            raw_count += 1
            normalized = normalize_gamma_market(item)
            if not normalized:
                increment_reason(reason_counts, "normalize_failed")
                continue
            normalized_count += 1
            cache_candidates.append(market_config_from_normalized(normalized))
            allowed, reject_reason = market_passes_filters(
                normalized,
                context.filters,
                context.risk.min_days_to_resolution,
                with_reason=True,
            )
            if not allowed:
                increment_reason(reason_counts, reject_reason or "filtered_out")
                continue
            filtered_candidates.append(normalized)
    save_market_cache_entries(context, cache_candidates)
    return filtered_candidates, raw_count, normalized_count, reason_counts


def scan_reward_candidates(context: BotContext) -> tuple[list[dict[str, Any]], int, int, dict[str, int]]:
    if not bool(context.filters.get("prefer_rewards", True)):
        return [], 0, 0, {}
    filtered_candidates: list[dict[str, Any]] = []
    normalized_rewards: list[dict[str, Any]] = []
    cache_candidates: list[MarketConfig] = []
    raw_count = 0
    normalized_count = 0
    reason_counts: dict[str, int] = {}
    next_cursor: Optional[str] = None
    page_size = min(max(context.scan_limit, 1), 50)
    for _ in range(context.scan_pages):
        try:
            payload = context.client.scan_reward_markets(
                page_size=page_size,
                next_cursor=next_cursor,
                tag_slugs=None,
                order_by="rate_per_day",
                position="DESC",
                min_volume_24hr=None,
                max_volume_24hr=None,
                min_price=None,
                max_price=None,
            )
        except Exception as exc:
            context.analytics.log_event("scan_error", {"details": str(exc), "source": "rewards"})
            increment_reason(reason_counts, "rewards_scan_error")
            print(f"Rewards scan failed: {exc}")
            return [], 0, 0, reason_counts
        data = payload.get("data") or []
        if not isinstance(data, list):
            break
        raw_count += len(data)
        for item in data:
            normalized = normalize_reward_market(item)
            if not normalized:
                increment_reason(reason_counts, "normalize_failed")
                continue
            normalized_count += 1
            normalized_rewards.append(normalized)
        next_cursor = payload.get("next_cursor")
        if not next_cursor or next_cursor == "LTE=":
            break
    if not normalized_rewards:
        return [], raw_count, normalized_count, reason_counts

    gamma_index = build_gamma_reward_metadata_index(context, normalized_rewards)
    for normalized in normalized_rewards:
        enrich_reward_market_with_gamma(normalized, gamma_index)
        cache_candidates.append(market_config_from_normalized(normalized))
        allowed, reject_reason = market_passes_filters(
            normalized,
            context.filters,
            context.risk.min_days_to_resolution,
            with_reason=True,
        )
        if not allowed:
            increment_reason(reason_counts, reject_reason or "filtered_out")
            continue
        max_competitiveness = _to_optional_float(context.filters.get("max_competitiveness"))
        competitiveness = _to_optional_float(normalized.get("market_competitiveness"))
        if (
            max_competitiveness is not None
            and competitiveness is not None
            and competitiveness > max_competitiveness
        ):
            increment_reason(reason_counts, "competitiveness_too_high")
            continue
        min_reward_rate = _to_optional_float(context.filters.get("min_reward_rate_per_day"))
        reward_rate = _to_optional_float(normalized.get("reward_rate_per_day"))
        if min_reward_rate is not None and (reward_rate or 0.0) < min_reward_rate:
            increment_reason(reason_counts, "reward_rate_too_low")
            continue
        reward_min_size = _to_optional_float(normalized.get("rewards_min_size"))
        if reward_min_size is not None and configured_quote_capacity(context) + 1e-9 < reward_min_size:
            increment_reason(reason_counts, "reward_min_size_above_budget")
            continue
        filtered_candidates.append(normalized)
    save_market_cache_entries(context, cache_candidates)
    return filtered_candidates, raw_count, normalized_count, reason_counts


def prebook_candidate_priority(candidate: dict[str, Any]) -> tuple[float, float, float]:
    reward_rate = _to_optional_float(candidate.get("reward_rate_per_day")) or 0.0
    competitiveness = _to_optional_float(candidate.get("market_competitiveness"))
    competition_score = 1.0 / (1.0 + max(competitiveness or 0.0, 0.0))
    return (
        reward_rate * competition_score,
        competition_score,
        float(candidate.get("volume_24h", 0.0)),
    )


def build_gamma_reward_metadata_index(
    context: BotContext,
    reward_markets: list[dict[str, Any]],
) -> dict[str, dict[str, dict[str, Any]]]:
    target_slugs = {str(market.get("slug") or "").strip().lower() for market in reward_markets if market.get("slug")}
    target_token_ids = {
        str(token_id).strip()
        for market in reward_markets
        for token_id in (market.get("yes_token_id"), market.get("no_token_id"))
        if token_id
    }
    by_slug: dict[str, dict[str, Any]] = {}
    by_token_id: dict[str, dict[str, Any]] = {}

    for slug in sorted(target_slugs):
        try:
            direct = context.client.fetch_gamma_market_by_slug(slug)
        except Exception as exc:
            context.analytics.log_event("reward_gamma_lookup_error", {"slug": slug, "details": str(exc)})
            continue
        normalized = normalize_gamma_market(direct) if direct else None
        if not normalized:
            continue
        by_slug[slug] = normalized
        yes_token_id = str(normalized.get("yes_token_id") or "").strip()
        no_token_id = str(normalized.get("no_token_id") or "").strip()
        if yes_token_id:
            by_token_id[yes_token_id] = normalized
        if no_token_id:
            by_token_id[no_token_id] = normalized

    remaining_slugs = {slug for slug in target_slugs if slug not in by_slug}
    remaining_token_ids = {token_id for token_id in target_token_ids if token_id not in by_token_id}
    if not remaining_slugs and not remaining_token_ids:
        return {"by_slug": by_slug, "by_token_id": by_token_id}

    gamma_pages = max(context.scan_pages, 15)

    for page in range(gamma_pages):
        try:
            raw_markets = context.client.scan_markets(limit=context.scan_limit, offset=page * context.scan_limit)
        except Exception as exc:
            context.analytics.log_event("reward_gamma_enrichment_scan_error", {"page": page, "details": str(exc)})
            continue
        for item in raw_markets:
            normalized = normalize_gamma_market(item)
            if not normalized:
                continue
            slug = str(normalized.get("slug") or "").strip().lower()
            yes_token_id = str(normalized.get("yes_token_id") or "").strip()
            no_token_id = str(normalized.get("no_token_id") or "").strip()
            matched = False
            if slug and slug in remaining_slugs:
                by_slug[slug] = normalized
                matched = True
            if yes_token_id and yes_token_id in target_token_ids:
                by_token_id[yes_token_id] = normalized
                matched = True
            if no_token_id and no_token_id in target_token_ids:
                by_token_id[no_token_id] = normalized
                matched = True
            if matched:
                continue
        if len(by_slug) >= len(target_slugs) and len(by_token_id) >= len(target_token_ids):
            break
    return {"by_slug": by_slug, "by_token_id": by_token_id}


def enrich_reward_market_with_gamma(
    reward_market: dict[str, Any],
    gamma_index: dict[str, dict[str, dict[str, Any]]],
) -> None:
    by_slug = gamma_index.get("by_slug", {})
    by_token_id = gamma_index.get("by_token_id", {})
    slug = str(reward_market.get("slug") or "").strip().lower()
    yes_token_id = str(reward_market.get("yes_token_id") or "").strip()
    no_token_id = str(reward_market.get("no_token_id") or "").strip()
    gamma_market = (
        by_slug.get(slug)
        or by_token_id.get(yes_token_id)
        or by_token_id.get(no_token_id)
    )
    if not gamma_market:
        return
    if not reward_market.get("question") and gamma_market.get("question"):
        reward_market["question"] = gamma_market["question"]
    if not reward_market.get("category") and gamma_market.get("category"):
        reward_market["category"] = gamma_market["category"]
    if not reward_market.get("end_date") and gamma_market.get("end_date"):
        reward_market["end_date"] = gamma_market["end_date"]
    if not reward_market.get("reference_price") and gamma_market.get("reference_price") is not None:
        reward_market["reference_price"] = gamma_market["reference_price"]
    gamma_volume = _to_optional_float(gamma_market.get("volume_24h"))
    reward_volume = _to_optional_float(reward_market.get("volume_24h"))
    if gamma_volume is not None and (reward_volume is None or gamma_volume > reward_volume):
        reward_market["volume_24h"] = gamma_volume
    if reward_market.get("neg_risk") is None and gamma_market.get("neg_risk") is not None:
        reward_market["neg_risk"] = gamma_market["neg_risk"]


def market_config_from_normalized(normalized: dict[str, Any]) -> MarketConfig:
    return MarketConfig(
        slug=normalized["slug"],
        question=normalized.get("question") or normalized["slug"],
        yes_token_id=normalized["yes_token_id"],
        no_token_id=normalized["no_token_id"],
        end_date=parse_end_date(normalized.get("end_date")),
        neg_risk=normalized.get("neg_risk"),
        condition_id=normalized.get("condition_id"),
        reward_rate_per_day=_to_optional_float(normalized.get("reward_rate_per_day")),
        rewards_min_size=_to_optional_float(normalized.get("rewards_min_size")),
        rewards_max_spread=_to_optional_float(normalized.get("rewards_max_spread")),
        market_competitiveness=_to_optional_float(normalized.get("market_competitiveness")),
    )


def merge_selected_markets(context: BotContext, ranked_candidates: list[MarketConfig]) -> list[MarketConfig]:
    selected: list[MarketConfig] = []
    selected_slugs: set[str] = set()
    family_counts: dict[str, int] = {}
    ranked_by_slug = {market.slug: market for market in ranked_candidates}
    max_markets_per_family = max(int(context.filters.get("max_markets_per_family", 0) or 0), 0)

    for market in context.positions_to_unwind:
        if market.slug in context.released_markets:
            continue
        inventory = context.inventory.get(market.slug)
        if inventory_requires_active_unwind(context, market, inventory) and market.slug not in selected_slugs:
            selected.append(ranked_by_slug.get(market.slug, market))
            selected_slugs.add(market.slug)
            family = market_family_key(market)
            family_counts[family] = family_counts.get(family, 0) + 1

    for market in context.active_markets:
        if market.slug in context.released_markets:
            continue
        inventory = context.inventory.get(market.slug)
        if inventory_requires_active_unwind(context, market, inventory) and market.slug not in selected_slugs:
            selected.append(ranked_by_slug.get(market.slug, market))
            selected_slugs.add(market.slug)
            family = market_family_key(market)
            family_counts[family] = family_counts.get(family, 0) + 1

    for market in ranked_candidates:
        if len(selected) >= context.max_active_markets:
            break
        if market.slug in selected_slugs:
            continue
        if market.slug in context.released_markets:
            continue
        inventory = context.inventory.get(market.slug)
        if inventory.gross_shares > 0.01 and not inventory_requires_active_unwind(context, market, inventory):
            continue
        if market_in_reentry_cooldown(context, market) and context.inventory.get(market.slug).gross_shares <= 0.01:
            continue
        family = market_family_key(market)
        if max_markets_per_family > 0 and family_counts.get(family, 0) >= max_markets_per_family:
            continue
        selected.append(market)
        selected_slugs.add(market.slug)
        family_counts[family] = family_counts.get(family, 0) + 1

    return selected[: context.max_active_markets]


def market_family_key(market: MarketConfig) -> str:
    base_text = f"{market.slug} {market.question}".lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", base_text)
    stopwords = {
        "will",
        "the",
        "a",
        "an",
        "and",
        "or",
        "of",
        "on",
        "to",
        "by",
        "for",
        "between",
        "over",
        "under",
        "more",
        "less",
        "than",
        "get",
        "gets",
        "video",
        "next",
        "views",
        "day",
        "week",
        "million",
    }
    tokens = [
        token
        for token in cleaned.split()
        if token and not token.isdigit() and token not in stopwords and len(token) > 2
    ]
    if not tokens:
        return market.slug
    return " ".join(tokens[:2])


def sync_positions_to_unwind(context: BotContext, report: Any) -> None:
    discovered_markets = discover_markets_for_unknown_positions(context, getattr(report, "unknown_positions", {}))
    context.markets = merge_unique_markets(context.markets, discovered_markets)
    known_by_slug = {market.slug: market for market in markets_for_state_tracking(context)}

    positions: list[MarketConfig] = []
    for market in known_by_slug.values():
        if market.slug in context.released_markets:
            continue
        inventory = context.inventory.get(market.slug)
        if is_non_actionable_dust_position(context, market, inventory):
            maybe_handle_dust_position(context, market, inventory)
            continue
        if inventory.gross_shares > 0.01:
            positions.append(market)
    positions.sort(key=lambda market: context.inventory.get(market.slug).gross_shares, reverse=True)
    context.positions_to_unwind = positions
    save_discovered_position_markets(context)
    save_market_cache(context)


def sync_positions_to_unwind_from_inventory(context: BotContext) -> None:
    known_by_slug = {market.slug: market for market in markets_for_state_tracking(context)}
    positions: list[MarketConfig] = []
    for market in known_by_slug.values():
        if market.slug in context.released_markets:
            continue
        inventory = context.inventory.get(market.slug)
        if is_non_actionable_dust_position(context, market, inventory):
            maybe_handle_dust_position(context, market, inventory)
            continue
        if inventory.gross_shares > 0.01:
            positions.append(market)
    positions.sort(key=lambda market: context.inventory.get(market.slug).gross_shares, reverse=True)
    context.positions_to_unwind = positions
    save_discovered_position_markets(context)
    save_market_cache(context)


def discover_markets_for_unknown_positions(context: BotContext, unknown_positions: dict[str, float]) -> list[MarketConfig]:
    target_token_ids = {token_id for token_id, balance in unknown_positions.items() if float(balance or 0.0) > 0.01}
    if not target_token_ids:
        return []

    discovered: list[MarketConfig] = []
    matched_tokens: set[str] = set()
    cached_markets = load_cached_markets(context.market_cache_path)

    for market in cached_markets:
        matched_here = False
        inventory = context.inventory.get(market.slug)
        if market.yes_token_id in target_token_ids:
            inventory.yes_shares = max(float(unknown_positions[market.yes_token_id]), 0.0)
            matched_tokens.add(market.yes_token_id)
            matched_here = True
        if market.no_token_id in target_token_ids:
            inventory.no_shares = max(float(unknown_positions[market.no_token_id]), 0.0)
            matched_tokens.add(market.no_token_id)
            matched_here = True
        if matched_here and all(existing.slug != market.slug for existing in discovered):
            discovered.append(market)

    reward_cursor: Optional[str] = None
    reward_page_size = min(max(context.scan_limit, 1), 50)
    reward_pages = max(context.scan_pages, 8)

    for _ in range(reward_pages):
        if matched_tokens == target_token_ids:
            break
        try:
            payload = context.client.scan_reward_markets(
                page_size=reward_page_size,
                next_cursor=reward_cursor,
                order_by="rate_per_day",
                position="DESC",
            )
        except Exception as exc:
            context.analytics.log_event("position_discovery_rewards_error", {"details": str(exc)})
            break
        data = payload.get("data") or []
        if not isinstance(data, list) or not data:
            break
        normalized_rewards: list[dict[str, Any]] = []
        for item in data:
            normalized = normalize_reward_market(item)
            if normalized:
                normalized_rewards.append(normalized)
        gamma_index = build_gamma_reward_metadata_index(context, normalized_rewards) if normalized_rewards else {}
        for normalized in normalized_rewards:
            enrich_reward_market_with_gamma(normalized, gamma_index)
            yes_token_id = str(normalized.get("yes_token_id") or "").strip()
            no_token_id = str(normalized.get("no_token_id") or "").strip()
            if yes_token_id not in target_token_ids and no_token_id not in target_token_ids:
                continue
            market = market_config_from_normalized(normalized)
            inventory = context.inventory.get(market.slug)
            matched_here = False
            if yes_token_id in target_token_ids:
                inventory.yes_shares = max(float(unknown_positions[yes_token_id]), 0.0)
                matched_tokens.add(yes_token_id)
                matched_here = True
            if no_token_id in target_token_ids:
                inventory.no_shares = max(float(unknown_positions[no_token_id]), 0.0)
                matched_tokens.add(no_token_id)
                matched_here = True
            if matched_here and all(existing.slug != market.slug for existing in discovered):
                discovered.append(market)
        reward_cursor = payload.get("next_cursor")
        if not reward_cursor or reward_cursor == "LTE=":
            break

    scan_pages = max(context.scan_pages, 25)

    for page in range(scan_pages):
        if matched_tokens == target_token_ids:
            break
        try:
            raw_markets = context.client.scan_markets(limit=context.scan_limit, offset=page * context.scan_limit)
        except Exception as exc:
            context.analytics.log_event("position_discovery_scan_error", {"page": page, "details": str(exc)})
            continue
        for item in raw_markets:
            normalized = normalize_gamma_market(item)
            if not normalized:
                continue
            yes_token_id = normalized["yes_token_id"]
            no_token_id = normalized["no_token_id"]
            matched_here = False
            market = MarketConfig(
                slug=normalized["slug"],
                question=normalized["question"],
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                end_date=parse_end_date(normalized.get("end_date")),
                neg_risk=normalized.get("neg_risk"),
            )
            inventory = context.inventory.get(market.slug)
            if yes_token_id in target_token_ids:
                inventory.yes_shares = max(float(unknown_positions[yes_token_id]), 0.0)
                matched_tokens.add(yes_token_id)
                matched_here = True
            if no_token_id in target_token_ids:
                inventory.no_shares = max(float(unknown_positions[no_token_id]), 0.0)
                matched_tokens.add(no_token_id)
                matched_here = True
            if matched_here and all(existing.slug != market.slug for existing in discovered):
                discovered.append(market)

    unresolved = sorted(target_token_ids - matched_tokens)
    if unresolved:
        now = time.time()
        newly_logged: list[str] = []
        for token_id in unresolved:
            last_logged_at = context.unresolved_position_logged_at.get(token_id, 0.0)
            if now - last_logged_at >= 3600.0:
                context.unresolved_position_logged_at[token_id] = now
                newly_logged.append(token_id)
        if newly_logged:
            context.analytics.log_event("position_discovery_unresolved", {"token_ids": newly_logged})
    if discovered:
        save_market_cache_entries(context, discovered)
    return discovered


def is_unwind_priority_market(context: BotContext, market: MarketConfig) -> bool:
    return any(existing.slug == market.slug for existing in context.positions_to_unwind)


def market_in_reentry_cooldown(context: BotContext, market: MarketConfig) -> bool:
    expires_at = context.reentry_cooldowns.get(market.slug)
    if expires_at is None:
        return False
    if expires_at <= time.time():
        context.reentry_cooldowns.pop(market.slug, None)
        save_reentry_cooldowns(context)
        return False
    return True


def market_passes_filters(
    market: dict[str, Any],
    filters: dict[str, Any],
    min_days: float,
    with_reason: bool = False,
) -> bool | tuple[bool, Optional[str]]:
    category = (market.get("category") or infer_market_category(market)).lower()
    allowed_categories = {entry.lower() for entry in filters.get("categories", [])}
    excluded_categories = {entry.lower() for entry in filters.get("excluded_categories", [])}
    excluded_keywords = {entry.lower() for entry in filters.get("excluded_keywords", [])}
    searchable_text = f"{market.get('slug', '')} {market.get('question', '')}".lower()
    if category in excluded_categories:
        return reject_filter_result(with_reason, "excluded_category")
    if any(keyword in searchable_text for keyword in excluded_keywords):
        return reject_filter_result(with_reason, "excluded_keyword")
    if allowed_categories:
        if not category:
            return reject_filter_result(with_reason, "category_not_allowed")
        if category not in allowed_categories:
            return reject_filter_result(with_reason, "category_not_allowed")
    if not market.get("accepting_orders", True):
        return reject_filter_result(with_reason, "not_accepting_orders")
    volume_24h = float(market.get("volume_24h", 0.0))
    if volume_24h < float(filters.get("min_volume_24h", 0.0)):
        return reject_filter_result(with_reason, "volume_too_low")
    max_volume_24h = _to_optional_float(filters.get("max_volume_24h"))
    if max_volume_24h is not None and volume_24h > max_volume_24h:
        return reject_filter_result(with_reason, "volume_too_high")
    reference_price = market.get("reference_price")
    if fair_value_band_breach(reference_price, filters) is not None:
        return reject_filter_result(with_reason, "fair_value_out_of_band")
    if bool(filters.get("rewards_only", False)) and (_to_optional_float(market.get("reward_rate_per_day")) or 0.0) <= 0.0:
        return reject_filter_result(with_reason, "not_reward_market")
    end_date = parse_end_date(market.get("end_date"))
    if min_days > 0 and end_date is None:
        return reject_filter_result(with_reason, "missing_end_date")
    if end_date is not None:
        days_left = max((end_date - datetime.now(timezone.utc)).total_seconds() / 86400.0, 0.0)
        if days_left < min_days:
            return reject_filter_result(with_reason, "too_close_to_resolution")
    return (True, None) if with_reason else True


def reject_filter_result(with_reason: bool, reason: str) -> bool | tuple[bool, Optional[str]]:
    return (False, reason) if with_reason else False


def fair_value_band_breach(fair_value: Optional[float], filters: dict[str, Any]) -> Optional[str]:
    if fair_value is None:
        return None
    min_fair_value = _to_optional_float(filters.get("min_fair_value"))
    max_fair_value = _to_optional_float(filters.get("max_fair_value"))
    if min_fair_value is not None and fair_value < min_fair_value:
        return f"fair value below min bound ({fair_value:.3f} < {min_fair_value:.3f})"
    if max_fair_value is not None and fair_value > max_fair_value:
        return f"fair value above max bound ({fair_value:.3f} > {max_fair_value:.3f})"
    return None


def _to_optional_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def increment_reason(reason_counts: dict[str, int], reason: str) -> None:
    reason_counts[reason] = reason_counts.get(reason, 0) + 1


def format_reason_counts(reason_counts: dict[str, int]) -> str:
    if not reason_counts:
        return "none"
    ordered = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
    return ",".join(f"{reason}={count}" for reason, count in ordered)


def score_market_candidate(
    volume_24h: float,
    spread_bps: float,
    reward_rate_per_day: Optional[float] = None,
    market_competitiveness: Optional[float] = None,
    reward_min_size: Optional[float] = None,
    days_to_resolution: Optional[float] = None,
    sports_preference: bool = False,
) -> float:
    reward_rate = max(reward_rate_per_day or 0.0, 0.0)
    competition_penalty = 1.0 + max(market_competitiveness or 0.0, 0.0)
    size_penalty = 1.0 + max((reward_min_size or 0.0) / 100.0, 0.0)
    reward_component = (reward_rate * max(spread_bps, 1.0)) / (competition_penalty * size_penalty)
    liquidity_component = spread_bps * max(volume_24h, 1.0)
    duration_component = min(max(days_to_resolution or 0.0, 0.0), 60.0)
    sports_bonus = 25.0 if sports_preference else 0.0
    if reward_rate > 0:
        return reward_component * 1_000_000.0 + liquidity_component + duration_component + sports_bonus
    return liquidity_component + duration_component + sports_bonus


def parse_end_date(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    text = re.sub(r"([+-]\d{2})$", r"\1:00", text)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def days_to_resolution_from_value(value: Any) -> Optional[float]:
    end_date = parse_end_date(value)
    if end_date is None:
        return None
    return max((end_date - datetime.now(timezone.utc)).total_seconds() / 86400.0, 0.0)


def is_sports_market(market: dict[str, Any]) -> bool:
    category = str(market.get("category") or infer_market_category(market) or "").lower()
    question = str(market.get("question") or "").lower()
    slug = str(market.get("slug") or "").lower()
    return (
        category == "sports"
        or any(keyword in question for keyword in ("nba", "nfl", "mlb", "fifa", "premier league", "champions league"))
        or any(keyword in slug for keyword in ("nba", "nfl", "mlb", "fifa", "premier-league", "champions-league"))
    )


def infer_market_category(market: dict[str, Any]) -> str:
    text = f"{market.get('slug') or ''} {market.get('question') or ''}".lower()
    sports_keywords = (
        "nba", "nfl", "mlb", "nhl", "fifa", "uefa", "premier league", "champions league",
        "la liga", "serie a", "bundesliga", "world cup", "arsenal", "bayern", "psg",
        "real madrid", "barcelona", "lakers", "celtics", "thunder", "spurs", "finals",
    )
    politics_keywords = (
        "election", "president", "vote", "congress", "senate", "house", "trump", "romania",
        "prime minister", "parliament", "democratic", "republican", "nomination", "government",
    )
    crypto_keywords = (
        "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp", "doge", "crypto", "coinbase",
    )
    if any(keyword in text for keyword in sports_keywords):
        return "sports"
    if any(keyword in text for keyword in politics_keywords):
        return "politics"
    if any(keyword in text for keyword in crypto_keywords):
        return "crypto"
    return ""


def is_placeholder_market(market: MarketConfig) -> bool:
    return market.yes_token_id.startswith("REPLACE_WITH_REAL") or market.no_token_id.startswith("REPLACE_WITH_REAL")


def parse_usdc_amount(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value) / 1_000_000.0
    except (TypeError, ValueError):
        return 0.0
