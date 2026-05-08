"""Venue top-of-book fetchers for the paper arbitrage bot.

Bybit tokenized stocks are spot-only, so pure perp-versus-perp arbs are
restricted here to venues such as Extended, Hyperliquid, and GRVT.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib import request
from urllib.error import HTTPError

import websocket


LOG = logging.getLogger(__name__)
TIMEOUT_SECS = 10
USER_AGENT = "arb-bot/0.1 (paper)"


@dataclass(frozen=True)
class Quote:
    bid: float
    ask: float
    bid_size: float
    ask_size: float

    @property
    def mid(self) -> float:
        if self.bid <= 0 or self.ask <= 0:
            return 0.0
        return (self.bid + self.ask) / 2.0


_EXTENDED_CACHE_SECOND: int | None = None
_EXTENDED_CACHE_QUOTES: dict[str, Quote] = {}


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _get(url: str) -> Any | None:
    try:
        req = request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="GET",
        )
        with request.urlopen(req, timeout=TIMEOUT_SECS) as response:
            body = response.read().decode("utf-8")
        return json.loads(body) if body else None
    except Exception as exc:  # noqa: BLE001
        LOG.warning("GET %s failed: %s", url, exc)
        return None


def _post(url: str, body: Any) -> Any | None:
    try:
        payload = json.dumps(body).encode("utf-8")
        req = request.Request(
            url,
            data=payload,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )
        with request.urlopen(req, timeout=TIMEOUT_SECS) as response:
            body_text = response.read().decode("utf-8")
        return json.loads(body_text) if body_text else None
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8").strip()
        except Exception:  # noqa: BLE001
            detail = ""
        if detail:
            LOG.warning("POST %s failed: HTTP %s body=%s", url, exc.code, detail)
        else:
            LOG.warning("POST %s failed: %s", url, exc)
        return None
    except Exception as exc:  # noqa: BLE001
        LOG.warning("POST %s failed: %s", url, exc)
        return None


def _parse_level(level: Any) -> tuple[float, float]:
    if isinstance(level, dict):
        return _as_float(level.get("px") or level.get("price")), _as_float(
            level.get("sz") or level.get("size")
        )
    if isinstance(level, (list, tuple)) and level:
        price = _as_float(level[0])
        size = _as_float(level[1]) if len(level) > 1 else 0.0
        return price, size
    return 0.0, 0.0


def fetch_extended_all() -> dict[str, Quote]:
    global _EXTENDED_CACHE_QUOTES
    global _EXTENDED_CACHE_SECOND

    cache_second = int(time.time())
    if _EXTENDED_CACHE_SECOND == cache_second:
        return dict(_EXTENDED_CACHE_QUOTES)

    payload = _get("https://api.starknet.extended.exchange/api/v1/info/markets")
    markets = []
    if isinstance(payload, dict):
        candidate = payload.get("data")
        if isinstance(candidate, list):
            markets = candidate
    elif isinstance(payload, list):
        markets = payload

    quotes: dict[str, Quote] = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        if market.get("active") is False:
            continue

        symbol = str(market.get("symbol") or market.get("name") or "").strip()
        if not symbol:
            continue

        stats = market.get("marketStats") or market.get("market_stats") or {}
        bid = _as_float(stats.get("bidPrice") or stats.get("bid_price"))
        ask = _as_float(stats.get("askPrice") or stats.get("ask_price"))
        if bid <= 0 or ask <= 0:
            continue

        quotes[symbol] = Quote(
            bid=bid,
            ask=ask,
            bid_size=0.0,
            ask_size=0.0,
        )

    _EXTENDED_CACHE_SECOND = cache_second
    _EXTENDED_CACHE_QUOTES = quotes
    return dict(quotes)


def fetch_extended_book(symbol: str) -> Quote | None:
    return fetch_extended_all().get(symbol)


def fetch_hl_book(coin: str, dex: str) -> Quote | None:
    payload = _post(
        "https://api.hyperliquid.xyz/info",
        [{"type": "l2Book", "coin": f"{dex}:{coin}"}],
    )
    if payload is None:
        return None

    book = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(book, dict):
        return None

    levels = book.get("levels") or book.get("Levels") or []
    if not isinstance(levels, list) or len(levels) < 2:
        return None

    bid_level = levels[0][0] if levels[0] else None
    ask_level = levels[1][0] if levels[1] else None
    bid, bid_size = _parse_level(bid_level)
    ask, ask_size = _parse_level(ask_level)
    if bid <= 0 or ask <= 0:
        return None

    return Quote(bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size)


class _QuoteStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._quotes: dict[tuple[str, str], Quote] = {}

    def update(self, venue: str, symbol: str, quote: Quote) -> None:
        with self._lock:
            self._quotes[(venue, symbol)] = quote

    def get(self, venue: str, symbol: str) -> Quote | None:
        with self._lock:
            return self._quotes.get((venue, symbol))


class _ExtendedMarketDataThread(threading.Thread):
    def __init__(self, store: _QuoteStore, ws_base_url: str, user_agent: str) -> None:
        super().__init__(name="extended-market-data", daemon=True)
        self._store = store
        self._ws_base_url = ws_base_url.rstrip("/")
        self._user_agent = user_agent
        self._stop_event = threading.Event()
        self._ws_app: websocket.WebSocketApp | None = None

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:  # noqa: BLE001
                pass

    def run(self) -> None:
        ws_url = f"{self._ws_base_url}/stream.extended.exchange/v1/orderbooks?depth=1"
        while not self._stop_event.is_set():
            ws_app = websocket.WebSocketApp(
                ws_url,
                header=[f"User-Agent: {self._user_agent}"],
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws_app = ws_app
            try:
                ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:  # noqa: BLE001
                if not self._stop_event.is_set():
                    LOG.warning("Extended websocket failed: %s", exc)
            finally:
                self._ws_app = None

            if not self._stop_event.is_set():
                time.sleep(2.0)

    def _on_message(self, _ws, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return

        data = payload.get("data")
        if not isinstance(data, dict):
            return

        symbol = str(data.get("m") or "").strip()
        if not symbol:
            return

        bids = data.get("b") or []
        asks = data.get("a") or []
        if not bids or not asks:
            return

        bid_level = bids[0] if isinstance(bids[0], dict) else {}
        ask_level = asks[0] if isinstance(asks[0], dict) else {}
        bid = _as_float(bid_level.get("p"))
        ask = _as_float(ask_level.get("p"))
        bid_size = _as_float(bid_level.get("c") or bid_level.get("q"))
        ask_size = _as_float(ask_level.get("c") or ask_level.get("q"))
        if bid <= 0 or ask <= 0:
            return

        self._store.update(
            "extended",
            symbol,
            Quote(bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size),
        )

    def _on_error(self, _ws, error: Any) -> None:
        if not self._stop_event.is_set():
            LOG.warning("Extended websocket error: %s", error)

    def _on_close(self, _ws, status_code: Any, message: Any) -> None:
        if not self._stop_event.is_set():
            LOG.warning(
                "Extended websocket closed: code=%s message=%s",
                status_code,
                message,
            )


class _GrvtMarketDataThread(threading.Thread):
    def __init__(
        self,
        store: _QuoteStore,
        ws_url: str,
        user_agent: str,
        symbols: list[str],
    ) -> None:
        super().__init__(name="grvt-market-data", daemon=True)
        self._store = store
        self._ws_url = ws_url
        self._user_agent = user_agent
        self._symbols = list(symbols)
        self._stop_event = threading.Event()
        self._ws_app: websocket.WebSocketApp | None = None

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:  # noqa: BLE001
                pass

    def run(self) -> None:
        while not self._stop_event.is_set():
            ws_app = websocket.WebSocketApp(
                self._ws_url,
                header=[f"User-Agent: {self._user_agent}"],
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._ws_app = ws_app
            try:
                ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:  # noqa: BLE001
                if not self._stop_event.is_set():
                    LOG.warning("GRVT websocket failed: %s", exc)
            finally:
                self._ws_app = None

            if not self._stop_event.is_set():
                time.sleep(2.0)

    def _on_open(self, ws_app) -> None:
        selectors = [f"{symbol}@500" for symbol in self._symbols]
        subscribe = {
            "jsonrpc": "2.0",
            "method": "subscribe",
            "params": {
                "stream": "v1.mini.s",
                "selectors": selectors,
            },
            "id": int(time.time() * 1000),
        }
        ws_app.send(json.dumps(subscribe))

    def _on_message(self, _ws, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return

        feed = payload.get("feed")
        if not isinstance(feed, dict):
            feed = payload.get("f")
        if not isinstance(feed, dict):
            return

        symbol = str(feed.get("instrument") or feed.get("i") or "").strip()
        if not symbol:
            return

        bid = _as_float(feed.get("best_bid_price") or feed.get("bb"))
        ask = _as_float(feed.get("best_ask_price") or feed.get("ba"))
        bid_size = _as_float(feed.get("best_bid_size") or feed.get("bb1"))
        ask_size = _as_float(feed.get("best_ask_size") or feed.get("ba1"))
        if bid <= 0 or ask <= 0:
            return

        self._store.update(
            "grvt",
            symbol,
            Quote(bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size),
        )

    def _on_error(self, _ws, error: Any) -> None:
        if not self._stop_event.is_set():
            LOG.warning("GRVT websocket error: %s", error)

    def _on_close(self, _ws, status_code: Any, message: Any) -> None:
        if not self._stop_event.is_set():
            LOG.warning("GRVT websocket closed: code=%s message=%s", status_code, message)


class StreamingQuoteFeed:
    """Keeps the latest Extended and GRVT top-of-book in memory via websockets."""

    def __init__(self, venue_configs: dict[str, Any], base_symbols: list[str]) -> None:
        self._venue_configs = venue_configs
        self._base_symbols = list(base_symbols)
        self._store = _QuoteStore()
        self._threads: list[threading.Thread] = []
        self._stoppable_threads: list[Any] = []

    def start(self) -> None:
        self._prime_rest_snapshots()

        extended_config = self._venue_configs.get("extended")
        if extended_config is not None:
            extended_thread = _ExtendedMarketDataThread(
                self._store,
                getattr(extended_config, "endpoints", {}).get(
                    "ws_market_url",
                    "wss://api.starknet.extended.exchange",
                ),
                getattr(extended_config, "user_agent", USER_AGENT),
            )
            self._threads.append(extended_thread)
            self._stoppable_threads.append(extended_thread)
            extended_thread.start()

        grvt_config = self._venue_configs.get("grvt")
        if grvt_config is not None:
            grvt_symbols = [f"{base_symbol}_USDT_Perp" for base_symbol in self._base_symbols]
            grvt_thread = _GrvtMarketDataThread(
                self._store,
                getattr(grvt_config, "endpoints", {}).get(
                    "market_ws_url",
                    "wss://market-data.grvt.io/ws/full",
                ),
                getattr(grvt_config, "user_agent", USER_AGENT),
                grvt_symbols,
            )
            self._threads.append(grvt_thread)
            self._stoppable_threads.append(grvt_thread)
            grvt_thread.start()

    def stop(self) -> None:
        for thread in self._stoppable_threads:
            thread.stop()
        for thread in self._threads:
            thread.join(timeout=3.0)

    def get_quote(self, venue: str, symbol: str) -> Quote | None:
        quote = self._store.get(venue, symbol)
        if quote is not None:
            return quote

        fetcher = VENUE_FETCHERS.get(venue)
        if fetcher is None:
            return None
        if venue == "hl":
            dex, coin = symbol.split(":", 1)
            quote = fetcher(coin, dex)
        else:
            quote = fetcher(symbol)
        if quote is not None:
            self._store.update(venue, symbol, quote)
        return quote

    def _prime_rest_snapshots(self) -> None:
        for symbol, quote in fetch_extended_all().items():
            self._store.update("extended", symbol, quote)
        for base_symbol in self._base_symbols:
            symbol = f"{base_symbol}_USDT_Perp"
            quote = fetch_grvt_book(symbol)
            if quote is not None:
                self._store.update("grvt", symbol, quote)


def fetch_grvt_book(symbol: str) -> Quote | None:
    payload = _post(
        "https://market-data.grvt.io/full/v1/ticker",
        {"instrument": symbol},
    )
    if payload is None:
        return None

    item: Any = payload
    if isinstance(payload, list):
        item = payload[0] if payload else None
    elif isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, list):
            item = result[0] if result else None
        elif result is not None:
            item = result

    if not isinstance(item, dict):
        return None

    bid = _as_float(item.get("best_bid_price"))
    ask = _as_float(item.get("best_ask_price"))
    bid_size = _as_float(item.get("best_bid_size"))
    ask_size = _as_float(item.get("best_ask_size"))
    if bid <= 0 or ask <= 0:
        return None

    return Quote(bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size)


VENUE_FETCHERS: dict[str, Callable[..., Quote | None]] = {
    "extended": fetch_extended_book,
    "hl": fetch_hl_book,
    "grvt": fetch_grvt_book,
}
