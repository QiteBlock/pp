from __future__ import annotations

from pathlib import Path
import sys

import yaml
from py_clob_client_v2 import PartialCreateOrderOptions, Side

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.market_maker import load_context, select_best_market
from src.pricing import build_yes_quote
from src.signals import build_signal
from src.client import format_tick_size


def main() -> None:
    context = load_context("config.yaml")
    market = select_best_market(context)
    if market is None:
        raise SystemExit("No market selected")
    book = context.client.get_orderbook(market.yes_token_id)
    recent_prices = context.client.fetch_market_history(market.yes_token_id)
    signal = build_signal(book.midpoint, book.last_trade_price, recent_prices, market.end_date)
    quote = build_yes_quote(context.pricing, signal, 0.0, book.tick_size, book.spread)
    context.client.ensure_api_credentials()

    print(f"market={market.slug}")
    print(f"neg_risk={market.neg_risk}")
    if hasattr(context.client.trading_client, "get_version"):
        print(f"server_version={context.client.trading_client.get_version()}")
    else:
        print("server_version=v1-sdk")
    print(f"tick_size={book.tick_size} formatted={format_tick_size(book.tick_size)}")
    print(f"top=({book.best_bid},{book.best_ask}) quote=({quote.bid},{quote.ask})")
    for side, price in (("BUY", quote.bid), ("SELL", quote.ask)):
        if context.client.sdk == "v1":
            order_side = context.client.v1_buy_sell(side)
            order = context.client.trading_client.create_order(
                context.client.build_v1_order_args(market.yes_token_id, price, quote.size, order_side)
            )
        else:
            order = context.client.trading_client.create_order(
                context.client.build_order_args(
                    market.yes_token_id,
                    price,
                    quote.size,
                    Side.BUY if side == "BUY" else Side.SELL,
                ),
                PartialCreateOrderOptions(tick_size=format_tick_size(book.tick_size), neg_risk=market.neg_risk),
            )
        print(f"{side} order_class={order.__class__.__name__}")
        print(f"{side} signature_prefix={getattr(order, 'signature', '')[:12]}")


if __name__ == "__main__":
    main()
