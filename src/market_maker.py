from __future__ import annotations

from datetime import datetime, timezone
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import json

import yaml

from .analytics import AnalyticsWriter
from .client import MarketConfig, PolymarketClient, OrderBookSnapshot, normalize_gamma_market, parse_market_configs
from .fills import FillPoller
from .inventory import InventoryBook
from .order_duplicates import OrderDuplicateDetector
from .pricing import PricingConfig, build_yes_quote, clamp_probability, round_to_tick
from .reconcile import PositionReconciler
from .risk import RiskConfig, can_quote_market, exposure_by_market, total_exposure
from .signals import build_signal
from .telegram_bot import BotControl, TelegramController


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
        filters=config.get("filters", {}),
    )
    hydrate_discovered_position_markets(context)
    return context


def hydrate_discovered_position_markets(context: BotContext) -> None:
    persisted_markets = load_discovered_position_markets(context.discovered_positions_path)
    if not persisted_markets:
        return
    merged = merge_unique_markets(context.markets, persisted_markets)
    if len(merged) != len(context.markets):
        print(f"Loaded {len(merged) - len(context.markets)} persisted position market(s)")
    context.markets = merged


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
    return report


def run_recovery_reconciliation(context: BotContext, trigger: str, **details: Any) -> None:
    if context.dry_run:
        return
    report = perform_reconciliation(context, trigger=trigger, **details)
    print(f"Recovery reconciliation ({trigger}): {report.summary()}")


def process_market(context: BotContext, market: MarketConfig, mark_prices: dict[str, float]) -> None:
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
        )
        return
    inventory_bias = inventory.net_delta
    quote = build_yes_quote(
        context.pricing,
        signal,
        inventory_bias,
        book.tick_size,
        book.spread,
        best_bid=book.best_bid,
        best_ask=book.best_ask,
    )
    mark_prices[market.slug] = quote.fair_value
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
            )
            return
        context.analytics.log_event(
            "risk_skip",
            {"market": market.slug, "reason": fair_value_breach, "fair_value": quote.fair_value},
        )
        print(f"[{market.slug}] skipped: {fair_value_breach} fair={quote.fair_value:.3f}")
        return
    portfolio_exposure = total_exposure(context.inventory, mark_prices)
    contribution_map = exposure_by_market(context.inventory, mark_prices)
    largest_contributor = max(contribution_map, key=contribution_map.get, default=None)
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
            )
            return
        if should_trim_position(reason):
            process_unwind_market(context, market, inventory, quote, book, reason, mode="trim", aggressive=False)
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
            )
            return
        context.analytics.log_event("risk_skip", {"market": market.slug, "reason": reason})
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
            )
            return
        context.analytics.log_event(
            "risk_skip",
            {"market": market.slug, "reason": "portfolio exposure limit breached", "exposure": portfolio_exposure},
        )
        print(f"[{market.slug}] skipped: portfolio exposure limit breached exposure={portfolio_exposure:.4f}")
        return
    if not passes_post_only_guard(book, quote.bid, quote.ask):
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
        print(f"[{market.slug}] skipped: post-only guard blocked marketable quote ({quote.bid:.3f},{quote.ask:.3f})")
        return
    place_yes_bid, place_no_bid, quote_reason = determine_quote_sides(context, inventory)
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
        print(f"[{market.slug}] skipped: {quote_reason}")
        return

    no_bid_price = round_no_bid_price(quote.ask, book.tick_size)
    bid_response: Any = None
    ask_response: Any = None

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
) -> None:
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
    effective_aggressive = mode == "flatten" or aggressive or (
        mode == "trim" and inventory.unwind_cycles_without_fill >= context.unwind_escalation_cycles
    )
    order_request = build_unwind_order_request(
        context,
        market,
        inventory,
        quote,
        yes_book,
        mode=mode,
        aggressive=effective_aggressive,
    )
    if order_request is None:
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
        print(
            f"[{market.slug}] skipped: unable to build unwind order "
            f"yes={inventory.yes_shares:.4f} no={inventory.no_shares:.4f} delta={inventory.net_delta:.4f}"
        )
        return

    if not context.dry_run and order_request["side"].upper() == "SELL":
        if not prepare_sell_unwind_order(context, market, inventory, order_request):
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
            post_only=not effective_aggressive,
        )
    except Exception as exc:
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
    if mode == "trim":
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
) -> Optional[dict[str, Any]]:
    tick = max(yes_book.tick_size, 0.01)
    if mode == "flatten":
        return build_flatten_order_request(context, market, inventory, quote, yes_book, aggressive)
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
            "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive),
            "size": size,
            "tick_size": no_book.tick_size,
            "label": "NO trim ask",
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
            "price": derive_sell_price(yes_book, quote.fair_value, quote.half_spread, aggressive=aggressive),
            "size": size,
            "tick_size": tick,
            "label": "YES trim ask",
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
            "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive),
            "size": size,
            "tick_size": no_book.tick_size,
            "label": "NO trim ask",
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
            "price": derive_sell_price(yes_book, quote.fair_value, quote.half_spread, aggressive=aggressive),
            "size": size,
            "tick_size": tick,
            "label": "YES trim ask",
        }
    return None


def build_flatten_order_request(
    context: BotContext,
    market: MarketConfig,
    inventory: Any,
    quote: Any,
    yes_book: OrderBookSnapshot,
    aggressive: bool,
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
                    "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive),
                    "size": size,
                    "tick_size": no_book.tick_size,
                    "label": "NO flatten ask",
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
            "price": derive_sell_price(yes_book, quote.fair_value, quote.half_spread, aggressive=aggressive),
            "size": size,
            "tick_size": max(yes_book.tick_size, 0.01),
            "label": "YES flatten ask",
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
            "price": derive_sell_price(no_book, 1.0 - quote.fair_value, quote.half_spread, aggressive=aggressive),
            "size": size,
            "tick_size": no_book.tick_size,
            "label": "NO flatten ask",
        }
    return None


def bounded_unwind_size(requested_size: float, available_size: float, min_order_size: float) -> Optional[float]:
    available = max(float(available_size), 0.0)
    requested = max(float(requested_size), 0.0)
    if available <= 0.0 or requested <= 0.0:
        return None
    return min(requested, available)


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
            "details": details,
        },
    )
    print(
        f"[{market.slug}] {order_request['label']} rejected for insufficient balance/allowance; "
        f"live_balance={live_balance if live_balance is not None else 'unknown'}"
    )


def get_market_book(context: BotContext, token_id: str) -> Optional[OrderBookSnapshot]:
    try:
        return context.client.get_orderbook(token_id)
    except Exception as exc:
        context.analytics.log_event("orderbook_fetch_failed", {"token_id": token_id, "details": str(exc)})
        return None


def derive_sell_price(book: OrderBookSnapshot, fair_value: float, half_spread: float, aggressive: bool) -> float:
    if aggressive and book.best_bid is not None:
        return clamp_probability(round_to_tick(book.best_bid, book.tick_size, down=True))
    target = clamp_probability(fair_value + half_spread)
    price = round_to_tick(target, book.tick_size, down=False)
    if book.best_bid is not None:
        min_post_only = round_to_tick(book.best_bid + max(book.tick_size, 0.01), book.tick_size, down=False)
        price = max(price, min_post_only)
    return clamp_probability(price)


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
        yes_orders = normalize_open_orders(context.client.get_open_orders(asset_id=market.yes_token_id))
        no_orders = normalize_open_orders(context.client.get_open_orders(asset_id=market.no_token_id))
    except Exception as exc:
        print(f"  open orders check failed: {exc}")
        return
    orders = yes_orders + no_orders
    print(f"  open_orders={len(orders)}")
    for order in orders[:6]:
        print(
            "   "
            f"id={short_id(order.get('id'))} side={order.get('side')} outcome={order.get('outcome')} "
            f"price={order.get('price')} size={order.get('original_size') or order.get('size')}"
        )


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


def is_trading_restricted_error(exc: Exception) -> bool:
    return "Trading restricted in your region" in str(exc)


def is_insufficient_balance_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "not enough balance / allowance" in message or "balance is not enough" in message


def round_no_bid_price(yes_ask: float, tick_size: float) -> float:
    tick = max(tick_size, 0.01)
    raw = max(0.01, min(0.99, 1.0 - yes_ask))
    return round(int(raw / tick) * tick, 4)


def determine_quote_sides(context: BotContext, inventory: Any) -> tuple[bool, bool, str]:
    yes_limit = max(
        context.risk.min_order_size,
        context.risk.max_yes_shares_per_market * context.risk.requote_inventory_fraction_limit,
    )
    no_limit = max(
        context.risk.min_order_size,
        context.risk.max_no_shares_per_market * context.risk.requote_inventory_fraction_limit,
    )
    now = time.time()
    if (
        context.fill_cooldown_seconds > 0
        and inventory.last_fill_ts > 0
        and now - inventory.last_fill_ts < context.fill_cooldown_seconds
    ):
        remaining = int(max(context.fill_cooldown_seconds - (now - inventory.last_fill_ts), 0))
        if inventory.net_delta > 0.01:
            return False, True, f"post_fill_cooldown_rebalance_yes({remaining}s)"
        if inventory.net_delta < -0.01:
            return True, False, f"post_fill_cooldown_rebalance_no({remaining}s)"
        return False, False, f"post_fill_cooldown_flat({remaining}s)"

    place_yes_bid = inventory.yes_shares < yes_limit
    place_no_bid = inventory.no_shares < no_limit

    if place_yes_bid and place_no_bid:
        return True, True, "normal_quote"
    if place_yes_bid and not place_no_bid:
        return True, False, "inventory_rebalance_only_no_side_full"
    if place_no_bid and not place_yes_bid:
        return False, True, "inventory_rebalance_only_yes_side_full"
    return False, False, "inventory_limits_block_both_sides"


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
    if not context.auto_scan_enabled:
        return
    context.positions_to_unwind = [
        market for market in context.positions_to_unwind if context.inventory.get(market.slug).gross_shares > 0.01
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
        print("Selected markets:")
        for market in selected:
            print(f"  {market.slug} | {market.question}")


def get_markets_to_quote(context: BotContext) -> list[MarketConfig]:
    if context.auto_scan_enabled:
        return list(context.active_markets)
    return context.markets


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
    filtered_candidates: list[dict[str, Any]] = []
    raw_count = 0
    normalized_count = 0
    for page in range(context.scan_pages):
        try:
            raw_markets = context.client.scan_markets(limit=context.scan_limit, offset=page * context.scan_limit)
        except Exception as exc:
            context.analytics.log_event("scan_error", {"page": page, "details": str(exc)})
            continue
        for item in raw_markets:
            raw_count += 1
            normalized = normalize_gamma_market(item)
            if not normalized:
                continue
            normalized_count += 1
            if not market_passes_filters(normalized, context.filters, context.risk.min_days_to_resolution):
                continue
            filtered_candidates.append(normalized)
    if not filtered_candidates:
        fallback = merge_selected_markets(context, [])
        if fallback:
            print(f"Market scan found no candidates; keeping {len(fallback)} unwind market(s)")
            return fallback
        print(f"Market scan found no candidates: raw={raw_count} normalized={normalized_count}")
        return None
    filtered_candidates.sort(key=lambda item: float(item.get("volume_24h", 0.0)), reverse=True)
    shortlisted = filtered_candidates[: context.scan_candidate_cap]
    candidates: list[tuple[float, MarketConfig]] = []
    checked_books = 0
    for normalized in shortlisted:
        try:
            book = context.client.get_orderbook(normalized["yes_token_id"])
        except Exception as exc:
            context.analytics.log_event("scan_book_error", {"market": normalized["slug"], "details": str(exc)})
            continue
        checked_books += 1
        spread_bps = ((book.spread or 0.0) * 10000.0)
        if spread_bps < float(context.filters.get("min_spread_bps", 0.0)):
            continue
        live_reference_price = book.midpoint if book.midpoint is not None else normalized.get("reference_price")
        if fair_value_band_breach(live_reference_price, context.filters) is not None:
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
        )
        score = score_market_candidate(normalized["volume_24h"], spread_bps)
        candidates.append((score, market))
    if not candidates:
        fallback = merge_selected_markets(context, [])
        if fallback:
            print(
                "Market scan found no live-book candidates; "
                f"keeping {len(fallback)} unwind market(s): raw={raw_count} normalized={normalized_count} "
                f"filter_pass={len(filtered_candidates)} checked_books={checked_books}"
            )
            return fallback
        print(
            "Market scan found no live-book candidates: "
            f"raw={raw_count} normalized={normalized_count} filter_pass={len(filtered_candidates)} checked_books={checked_books}"
        )
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    selected = merge_selected_markets(context, [market for _, market in candidates])
    print(
        "Market scan selected "
        f"{len(selected)} markets: raw={raw_count} normalized={normalized_count} "
        f"filter_pass={len(filtered_candidates)} checked_books={checked_books}"
    )
    return selected


def merge_selected_markets(context: BotContext, ranked_candidates: list[MarketConfig]) -> list[MarketConfig]:
    selected: list[MarketConfig] = []
    selected_slugs: set[str] = set()
    ranked_by_slug = {market.slug: market for market in ranked_candidates}

    for market in context.positions_to_unwind:
        if context.inventory.get(market.slug).gross_shares > 0.01 and market.slug not in selected_slugs:
            selected.append(ranked_by_slug.get(market.slug, market))
            selected_slugs.add(market.slug)

    for market in context.active_markets:
        if context.inventory.get(market.slug).gross_shares > 0.01 and market.slug not in selected_slugs:
            selected.append(ranked_by_slug.get(market.slug, market))
            selected_slugs.add(market.slug)

    for market in ranked_candidates:
        if len(selected) >= context.max_active_markets:
            break
        if market.slug in selected_slugs:
            continue
        selected.append(market)
        selected_slugs.add(market.slug)

    return selected[: context.max_active_markets]


def sync_positions_to_unwind(context: BotContext, report: Any) -> None:
    discovered_markets = discover_markets_for_unknown_positions(context, getattr(report, "unknown_positions", {}))
    context.markets = merge_unique_markets(context.markets, discovered_markets)
    known_by_slug = {market.slug: market for market in markets_for_state_tracking(context)}

    positions: list[MarketConfig] = []
    for market in known_by_slug.values():
        if context.inventory.get(market.slug).gross_shares > 0.01:
            positions.append(market)
    positions.sort(key=lambda market: context.inventory.get(market.slug).gross_shares, reverse=True)
    context.positions_to_unwind = positions
    save_discovered_position_markets(context)


def sync_positions_to_unwind_from_inventory(context: BotContext) -> None:
    known_by_slug = {market.slug: market for market in markets_for_state_tracking(context)}
    positions: list[MarketConfig] = []
    for market in known_by_slug.values():
        if context.inventory.get(market.slug).gross_shares > 0.01:
            positions.append(market)
    positions.sort(key=lambda market: context.inventory.get(market.slug).gross_shares, reverse=True)
    context.positions_to_unwind = positions
    save_discovered_position_markets(context)


def discover_markets_for_unknown_positions(context: BotContext, unknown_positions: dict[str, float]) -> list[MarketConfig]:
    target_token_ids = {token_id for token_id, balance in unknown_positions.items() if float(balance or 0.0) > 0.01}
    if not target_token_ids:
        return []

    discovered: list[MarketConfig] = []
    matched_tokens: set[str] = set()
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
        context.analytics.log_event("position_discovery_unresolved", {"token_ids": unresolved})
    return discovered


def is_unwind_priority_market(context: BotContext, market: MarketConfig) -> bool:
    return any(existing.slug == market.slug for existing in context.positions_to_unwind)


def market_passes_filters(market: dict[str, Any], filters: dict[str, Any], min_days: float) -> bool:
    category = (market.get("category") or "").lower()
    allowed_categories = {entry.lower() for entry in filters.get("categories", [])}
    excluded_categories = {entry.lower() for entry in filters.get("excluded_categories", [])}
    excluded_keywords = {entry.lower() for entry in filters.get("excluded_keywords", [])}
    searchable_text = f"{market.get('slug', '')} {market.get('question', '')}".lower()
    if category in excluded_categories:
        return False
    if any(keyword in searchable_text for keyword in excluded_keywords):
        return False
    if category and allowed_categories and category not in allowed_categories:
        return False
    if not market.get("accepting_orders", True):
        return False
    if float(market.get("volume_24h", 0.0)) < float(filters.get("min_volume_24h", 0.0)):
        return False
    reference_price = market.get("reference_price")
    if fair_value_band_breach(reference_price, filters) is not None:
        return False
    end_date = parse_end_date(market.get("end_date"))
    if end_date is not None:
        days_left = max((end_date - datetime.now(timezone.utc)).total_seconds() / 86400.0, 0.0)
        if days_left < min_days:
            return False
    return True


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


def score_market_candidate(volume_24h: float, spread_bps: float) -> float:
    return spread_bps * max(volume_24h, 1.0)


def parse_end_date(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def is_placeholder_market(market: MarketConfig) -> bool:
    return market.yes_token_id.startswith("REPLACE_WITH_REAL") or market.no_token_id.startswith("REPLACE_WITH_REAL")


def parse_usdc_amount(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value) / 1_000_000.0
    except (TypeError, ValueError):
        return 0.0
