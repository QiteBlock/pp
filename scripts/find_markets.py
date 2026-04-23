from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.client import PolymarketClient, normalize_gamma_market


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan Polymarket Gamma API for market-making candidates.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--category", action="append", dest="categories", help="Override allowed category filter. Can be passed multiple times.")
    parser.add_argument("--min-volume", type=float, help="Override minimum 24h volume filter.")
    parser.add_argument("--min-spread-bps", type=float, help="Override minimum spread filter.")
    parser.add_argument("--min-days", type=float, default=7.0, help="Minimum days to resolution.")
    parser.add_argument("--candidate-cap", type=int, help="Only fetch orderbooks for the top N filtered markets by volume.")
    parser.add_argument("--no-book-filter", action="store_true", help="Print Gamma-filtered candidates without fetching orderbooks/spreads.")
    parser.add_argument("--debug", action="store_true", help="Print scan counts and sample filtered markets.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with open(args.config, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    client = PolymarketClient(config)
    filters = config["filters"]
    if args.categories:
        filters = {**filters, "categories": args.categories}
    if args.min_volume is not None:
        filters = {**filters, "min_volume_24h": args.min_volume}
    if args.min_spread_bps is not None:
        filters = {**filters, "min_spread_bps": args.min_spread_bps}
    candidate_cap = int(args.candidate_cap or config["runtime"].get("scan_candidate_cap", 25))
    matches: list[dict] = []
    stats = {"raw": 0, "normalized": 0, "filter_pass": 0, "spread_pass": 0}
    debug_examples: list[str] = []
    filtered_candidates: list[dict] = []
    for page in range(args.pages):
        try:
            raw = client.scan_markets(limit=args.limit, offset=page * args.limit)
        except Exception as exc:
            if args.debug:
                print(f"scan page {page} failed: {exc}")
            continue
        for item in raw:
            stats["raw"] += 1
            normalized = normalize_gamma_market(item)
            if not normalized:
                if args.debug and len(debug_examples) < 5:
                    debug_examples.append(f"normalize_skip slug={item.get('slug')} token_ids={item.get('clobTokenIds')}")
                continue
            stats["normalized"] += 1
            if not passes_filters(normalized, filters, args.min_days):
                if args.debug and len(debug_examples) < 10:
                    debug_examples.append(
                        f"filter_skip slug={normalized['slug']} category={normalized['category']} volume={normalized['volume_24h']:.0f} end={normalized['end_date']}"
                    )
                continue
            stats["filter_pass"] += 1
            filtered_candidates.append(normalized)
    filtered_candidates.sort(key=lambda item: float(item.get("volume_24h", 0.0)), reverse=True)
    if args.no_book_filter:
        print("slug,category,volume_24h,neg_risk,end_date,question")
        for item in filtered_candidates[:candidate_cap]:
            print(
                f"{item['slug']},{item['category']},{item['volume_24h']:.0f},"
                f"{item.get('neg_risk')},{item['end_date']},{item['question'].replace(',', ' ')}"
            )
        if not filtered_candidates:
            print("# no Gamma-filtered candidates")
        print(f"# raw={stats['raw']} normalized={stats['normalized']} filter_pass={stats['filter_pass']} printed={min(len(filtered_candidates), candidate_cap)}")
        return

    for normalized in filtered_candidates[:candidate_cap]:
        try:
            book = client.get_orderbook(normalized["yes_token_id"])
        except Exception as exc:
            if args.debug and len(debug_examples) < 15:
                debug_examples.append(f"book_skip slug={normalized['slug']} error={exc}")
            continue
        spread_bps = 0.0
        if book.spread is not None:
            spread_bps = book.spread * 10000
        if spread_bps < filters["min_spread_bps"]:
            if args.debug and len(debug_examples) < 15:
                debug_examples.append(
                    f"spread_skip slug={normalized['slug']} spread_bps={spread_bps:.1f} bid={book.best_bid} ask={book.best_ask}"
                )
            continue
        stats["spread_pass"] += 1
        matches.append(
            {
                **normalized,
                "spread_bps": round(spread_bps, 1),
                "best_bid": book.best_bid,
                "best_ask": book.best_ask,
            }
        )
    matches.sort(key=lambda item: (item["spread_bps"], item["volume_24h"]), reverse=True)
    print("slug,category,volume_24h,spread_bps,best_bid,best_ask,neg_risk,end_date,question")
    for item in matches:
        print(
            f"{item['slug']},{item['category']},{item['volume_24h']:.0f},{item['spread_bps']:.1f},"
            f"{item['best_bid']},{item['best_ask']},{item.get('neg_risk')},{item['end_date']},{item['question'].replace(',', ' ')}"
        )
    if not matches:
        print(
            f"# no matches after live orderbook spread filter. raw={stats['raw']} normalized={stats['normalized']} "
            f"filter_pass={stats['filter_pass']} checked_books={min(len(filtered_candidates), candidate_cap)}"
        )
    if args.debug:
        print()
        print(
            f"raw={stats['raw']} normalized={stats['normalized']} filter_pass={stats['filter_pass']} "
            f"candidate_cap={candidate_cap} spread_pass={stats['spread_pass']}"
        )
        for example in debug_examples:
            print(example)


def passes_filters(market: dict, filters: dict, min_days: float) -> bool:
    category = market["category"]
    allowed_categories = {entry.lower() for entry in filters["categories"]}
    excluded_categories = {entry.lower() for entry in filters["excluded_categories"]}
    excluded_keywords = {entry.lower() for entry in filters.get("excluded_keywords", [])}
    searchable_text = f"{market.get('slug', '')} {market.get('question', '')}".lower()
    if category in excluded_categories:
        return False
    if any(keyword in searchable_text for keyword in excluded_keywords):
        return False
    if category and allowed_categories and category not in allowed_categories:
        return False
    if not market["accepting_orders"]:
        return False
    if market["volume_24h"] < filters["min_volume_24h"]:
        return False
    end_date = market.get("end_date")
    if end_date:
        parsed = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        days_left = (parsed - datetime.now(timezone.utc)).total_seconds() / 86400.0
        if days_left < min_days:
            return False
    return True


if __name__ == "__main__":
    main()
