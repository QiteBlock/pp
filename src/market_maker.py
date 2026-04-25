from __future__ import annotations

from datetime import datetime, timezone
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

from .analytics import AnalyticsWriter
from .client import MarketConfig, PolymarketClient, OrderBookSnapshot, normalize_gamma_market, parse_market_configs
from .fills import FillPoller
from .inventory import InventoryBook
from .order_duplicates import OrderDuplicateDetector
from .pricing import PricingConfig, build_yes_quote
from .reconcile import PositionReconciler
from .risk import RiskConfig, can_quote_market, total_exposure
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
    dry_run: bool
    cancel_before_requote: bool
    auto_scan_enabled: bool
    scan_limit: int
    scan_pages: int
    scan_candidate_cap: int
    rescan_interval_seconds: int
    last_scan_ts: float
    active_market: Optional[MarketConfig]
    filters: dict[str, Any]


def run_market_maker(config_path: str) -> None:
    context = load_context(config_path)
    try:
        initialize_market_selection(context)
        print(f"Loaded {len(context.markets)} configured markets. Dry run={context.dry_run}. Auto-scan={context.auto_scan_enabled}.")
        if not context.dry_run:
            preflight_live_auth(context)
            reconciliation_report = context.reconciler.reconcile_startup(context.markets)
            print(f"Startup reconciliation: {reconciliation_report.summary()}")
            context.analytics.log_event("startup_reconciliation", {
                "status": reconciliation_report.status,
                "discrepancies_count": len(reconciliation_report.discrepancies),
                "adjustments_count": len(reconciliation_report.adjustments),
                "error": reconciliation_report.error,
            })
            if not reconciliation_report.should_proceed_with_trading():
                raise RuntimeError(f"Startup reconciliation failed - cannot proceed safely: {reconciliation_report.error}")
        if context.telegram.is_enabled():
            context.telegram.send_message(build_status_message(context, prefix="Bot process online"))
        while True:
            handle_telegram_commands(context)
            refresh_market_selection(context)
            mark_prices: dict[str, float] = {}

            if not context.dry_run:
                token_mapping = _build_token_mapping(context)
                fills_processed = context.fill_poller.poll_fills(token_mapping)
                if fills_processed > 0:
                    print(f"Processed {fills_processed} new fill(s)")

            if not context.control.trading_enabled:
                print("Trading paused by Telegram control.")
                time.sleep(context.loop_interval_seconds)
                continue

            if context.cancel_before_requote:
                context.client.cancel_all_orders(dry_run=context.dry_run)
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
                {"total_exposure_usdc": exposure, "markets": len(mark_prices), "active_market": context.active_market.slug if context.active_market else None},
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
    telegram = TelegramController(config.get("telegram", {}), runtime["log_dir"])
    fill_poller = FillPoller(client, inventory, analytics, runtime["log_dir"], fill_handler=telegram.notify_fill)
    reconciler = PositionReconciler(client, inventory)
    duplicate_detector = OrderDuplicateDetector(runtime["log_dir"])
    
    return BotContext(
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
        dry_run=bool(runtime["dry_run"]),
        cancel_before_requote=bool(runtime.get("cancel_before_requote", True)),
        auto_scan_enabled=bool(runtime.get("auto_scan_enabled", True)),
        scan_limit=int(runtime.get("scan_limit", 200)),
        scan_pages=int(runtime.get("scan_pages", 3)),
        scan_candidate_cap=int(runtime.get("scan_candidate_cap", 25)),
        rescan_interval_seconds=int(runtime.get("rescan_interval_seconds", 900)),
        last_scan_ts=0.0,
        active_market=None,
        filters=config.get("filters", {}),
    )


def _build_token_mapping(context: BotContext) -> dict[str, tuple[str, str]]:
    """Build mapping from token_id to (market_slug, outcome).
    
    Used for mapping API fills to inventory positions.
    """
    mapping = {}
    markets = list(context.markets)
    if context.active_market is not None and all(market.slug != context.active_market.slug for market in markets):
        markets.append(context.active_market)
    for market in markets:
        mapping[market.yes_token_id] = (market.slug, "YES")
        mapping[market.no_token_id] = (market.slug, "NO")
    return mapping


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
    inventory_bias = inventory.net_delta
    quote = build_yes_quote(context.pricing, signal, inventory_bias, book.tick_size, book.spread)
    allowed, reason = can_quote_market(context.risk, inventory, quote.fair_value, signal.time_to_resolution_days)
    mark_prices[market.slug] = quote.fair_value
    if not allowed:
        context.analytics.log_event("risk_skip", {"market": market.slug, "reason": reason})
        return
    portfolio_exposure = total_exposure(context.inventory, mark_prices)
    if portfolio_exposure > context.risk.max_total_exposure_usdc:
        context.analytics.log_event(
            "risk_skip",
            {"market": market.slug, "reason": "portfolio exposure limit breached", "exposure": portfolio_exposure},
        )
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
        no_bid_price = round_no_bid_price(quote.ask, book.tick_size)
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
        raise
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
            "responses": {"bid": bid_response, "ask": ask_response},
        },
    )
    print_quote_status(market, book, quote.fair_value, quote.bid, quote.ask, inventory.net_delta)
    print_order_response("YES bid", bid_response)
    print_order_response("NO bid", ask_response)
    print_open_orders(context, market)


def print_quote_status(market: MarketConfig, book: OrderBookSnapshot, fair_value: float, bid: float, ask: float, net_delta: float) -> None:
    no_bid = round_no_bid_price(ask, book.tick_size)
    print(
        f"[{market.slug}] fair={fair_value:.3f} "
        f"top=({book.best_bid},{book.best_ask}) yes_bid={bid:.3f} yes_ask={ask:.3f} no_bid={no_bid:.3f} delta={net_delta:.1f}"
    )


def print_order_response(label: str, response: Any) -> None:
    if isinstance(response, dict):
        order_id = response.get("orderID") or response.get("order_id") or response.get("id")
        status = response.get("status") or response.get("success") or response.get("errorMsg") or response.get("error")
        if order_id or status:
            print(f"  {label}: status={status} order_id={order_id}")
            return
    print(f"  {label}: response={response}")


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


def round_no_bid_price(yes_ask: float, tick_size: float) -> float:
    tick = max(tick_size, 0.01)
    raw = max(0.01, min(0.99, 1.0 - yes_ask))
    return round(int(raw / tick) * tick, 4)


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
        return
    context.markets = manual_markets
    refresh_market_selection(context, force=True)


def refresh_market_selection(context: BotContext, force: bool = False) -> None:
    if not context.auto_scan_enabled:
        return
    now = time.time()
    must_rescan = force or context.active_market is None or should_rescan_active_market(context)
    if not must_rescan and now - context.last_scan_ts < context.rescan_interval_seconds:
        return
    selected = select_best_market(context)
    context.last_scan_ts = now
    if selected is None:
        context.active_market = None
        context.analytics.log_event("selection_skip", {"reason": "no_candidate_found"})
        return
    if context.active_market is None or context.active_market.slug != selected.slug:
        previous = context.active_market.slug if context.active_market else None
        context.active_market = selected
        context.analytics.log_event(
            "market_switch",
            {"previous_market": previous, "new_market": selected.slug, "question": selected.question},
        )
        print(f"Selected market: {selected.slug} | {selected.question}")
        context.telegram.notify_market_switch(previous, selected.slug, selected.question)
    else:
        context.active_market = selected


def get_markets_to_quote(context: BotContext) -> list[MarketConfig]:
    if context.auto_scan_enabled:
        return [context.active_market] if context.active_market else []
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
    market = context.active_market
    if market is None:
        return True
    if market.end_date is None:
        return False
    days_left = max((market.end_date - datetime.now(timezone.utc)).total_seconds() / 86400.0, 0.0)
    return days_left < context.risk.min_days_to_resolution


def select_best_market(context: BotContext) -> Optional[MarketConfig]:
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
        print(
            "Market scan found no live-book candidates: "
            f"raw={raw_count} normalized={normalized_count} filter_pass={len(filtered_candidates)} checked_books={checked_books}"
        )
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    selected = candidates[0][1]
    print(
        "Market scan selected "
        f"{selected.slug}: raw={raw_count} normalized={normalized_count} filter_pass={len(filtered_candidates)} checked_books={checked_books}"
    )
    return selected


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
    end_date = parse_end_date(market.get("end_date"))
    if end_date is not None:
        days_left = max((end_date - datetime.now(timezone.utc)).total_seconds() / 86400.0, 0.0)
        if days_left < min_days:
            return False
    return True


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
        return float(value)
    except (TypeError, ValueError):
        return 0.0
