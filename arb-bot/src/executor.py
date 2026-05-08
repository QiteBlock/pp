"""Execution interfaces for paper and live trading modes."""

from __future__ import annotations

import asyncio
import csv
import importlib
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol
from urllib import request

from venues import Quote


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class FillReport:
    ts: str
    venue: str
    symbol: str
    side: str
    qty: float
    price: float
    fee_bps: float
    is_paper: bool


@dataclass(frozen=True)
class VenueConfig:
    venue: str
    allow_live: bool
    user_agent: str
    default_symbol: str
    taker_fee_bps: float
    maker_fee_bps: float
    notional_per_trade_usd: float
    max_position_usd: float
    credentials: dict[str, str]
    endpoints: dict[str, str]
    integration: dict[str, Any] = field(default_factory=dict)

    def missing_live_fields(self) -> list[str]:
        missing: list[str] = []
        if not self.allow_live:
            missing.append("bot.allow_live=false")
        required_keys: tuple[str, ...] = tuple(self.credentials.keys())
        if self.venue == "extended":
            required_keys = ("account_id", "api_key", "private_key")
        elif self.venue == "grvt":
            has_grvt_key = any(
                str(self.credentials.get(key, "")).strip()
                for key in ("wallet_private_key", "private_key")
            )
            if not has_grvt_key:
                missing.append("credentials.wallet_private_key")
            required_keys = tuple()
        for key in required_keys:
            value = self.credentials.get(key, "")
            if not str(value).strip():
                missing.append(f"credentials.{key}")
        return missing


class Executor(Protocol):
    def market_order(
        self,
        venue: str,
        symbol: str,
        side: str,
        qty: float,
        expected_quote: Quote,
        fee_bps: float,
    ) -> FillReport | None:
        ...

    def close(self) -> None:
        ...


class _CsvExecutorBase:
    def __init__(self, csv_path: str) -> None:
        self.csv_path = csv_path
        parent = os.path.dirname(csv_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._fh = open(csv_path, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(
            ["ts", "venue", "symbol", "side", "qty", "price", "fee_bps", "is_paper"]
        )
        self._fh.flush()

    def _append_report(self, report: FillReport) -> None:
        self._writer.writerow(
            [
                report.ts,
                report.venue,
                report.symbol,
                report.side,
                f"{report.qty:.8f}",
                f"{report.price:.8f}",
                f"{report.fee_bps:.4f}",
                int(report.is_paper),
            ]
        )
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class PaperExecutor(_CsvExecutorBase):
    def market_order(
        self,
        venue: str,
        symbol: str,
        side: str,
        qty: float,
        expected_quote: Quote,
        fee_bps: float,
    ) -> FillReport | None:
        side_lower = side.lower()
        if side_lower == "buy":
            price = expected_quote.ask
        elif side_lower == "sell":
            price = expected_quote.bid
        else:
            LOG.warning("Unsupported side %s for %s %s", side, venue, symbol)
            return None

        if price <= 0:
            LOG.warning("Skipping paper fill for %s %s because price <= 0", venue, symbol)
            return None

        report = FillReport(
            ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            venue=venue,
            symbol=symbol,
            side=side_lower,
            qty=qty,
            price=price,
            fee_bps=fee_bps,
            is_paper=True,
        )
        self._append_report(report)
        LOG.info(
            "PAPER FILL venue=%s symbol=%s side=%s qty=%.6f price=%.6f fee_bps=%.2f",
            venue,
            symbol,
            side_lower,
            qty,
            price,
            fee_bps,
        )
        return report


class RealExecutor(_CsvExecutorBase):
    """Live-execution seam.

    Uses first-party Python integrations:
    - GRVT via CCXT
    - Extended via x10-python-trading-starknet

    Order acknowledgements are recorded locally with the best available price
    from the API response, or the expected quote as fallback. Full fill
    reconciliation is still a later step.
    """

    IMPLEMENTED_LIVE_VENUES = frozenset({"extended", "grvt"})

    def __init__(self, csv_path: str, venue_configs: dict[str, VenueConfig]) -> None:
        super().__init__(csv_path)
        self.venue_configs = venue_configs
        self._clients: dict[str, Any] = {}

    def validate_pair_support(self, pairs) -> None:
        errors: list[str] = []
        for pair in pairs:
            for leg in (pair.long_leg, pair.short_leg):
                config = self.venue_configs.get(leg.venue)
                if config is None:
                    errors.append(f"{leg.venue}:{leg.symbol} missing config")
                    continue

                missing = config.missing_live_fields()
                if missing:
                    errors.append(f"{leg.venue}:{leg.symbol} not live-enabled ({', '.join(missing)})")
                    continue

                if leg.venue not in self.IMPLEMENTED_LIVE_VENUES:
                    errors.append(
                        f"{leg.venue}:{leg.symbol} signed live execution is not implemented yet"
                    )
                    continue

                dependency_error = self._dependency_error_for_venue(leg.venue)
                if dependency_error is not None:
                    errors.append(f"{leg.venue}:{leg.symbol} {dependency_error}")

        if errors:
            raise RuntimeError("Live mode blocked: " + "; ".join(dict.fromkeys(errors)))

    def _dependency_error_for_venue(self, venue: str) -> str | None:
        if venue == "grvt":
            if importlib.util.find_spec("ccxt") is None:
                return "requires `ccxt` to be installed"
            return None
        if venue == "extended":
            if importlib.util.find_spec("x10") is None:
                return "requires `x10-python-trading-starknet` to be installed"
            return None
        return f"has no live dependency mapping for venue {venue}"

    def _quote_price_for_side(self, side: str, expected_quote: Quote) -> float:
        side_lower = side.lower()
        if side_lower == "buy":
            return expected_quote.ask
        if side_lower == "sell":
            return expected_quote.bid
        raise RuntimeError(f"Unsupported side {side}")

    def _append_live_report(
        self,
        venue: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        fee_bps: float,
        note: str,
    ) -> FillReport:
        report = FillReport(
            ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            venue=venue,
            symbol=symbol,
            side=side.lower(),
            qty=qty,
            price=price,
            fee_bps=fee_bps,
            is_paper=False,
        )
        self._append_report(report)
        LOG.warning(
            "LIVE ORDER ACK venue=%s symbol=%s side=%s qty=%.6f price=%.6f %s",
            venue,
            symbol,
            side.lower(),
            qty,
            price,
            note,
        )
        return report

    def _get_grvt_client(self):
        client = self._clients.get("grvt")
        if client is not None:
            return client

        ccxt = importlib.import_module("ccxt")
        exchange_class = getattr(ccxt, "grvt")
        config = self.venue_configs["grvt"]
        wallet_private_key = (
            config.credentials.get("wallet_private_key")
            or config.credentials.get("private_key")
        )
        options = {
            "accountId": config.credentials.get("sub_account_id") or None,
            "builderFee": bool(config.integration.get("builder_fee", False)),
        }
        client = exchange_class(
            {
                "privateKey": wallet_private_key,
                "enableRateLimit": True,
                "timeout": int(float(config.integration.get("timeout_ms", 30000))),
                "options": options,
            }
        )
        client.load_markets()
        self._clients["grvt"] = client
        return client

    def _resolve_grvt_symbol(self, client, raw_symbol: str) -> str:
        market = client.markets_by_id.get(raw_symbol)
        if isinstance(market, list):
            market = market[0] if market else None
        if isinstance(market, dict):
            unified = market.get("symbol")
            if unified:
                return unified

        for unified_symbol, candidate in client.markets.items():
            if candidate.get("id") == raw_symbol:
                return unified_symbol

        raise RuntimeError(f"GRVT symbol {raw_symbol} not found in CCXT markets")

    def _extract_ccxt_order_price(self, order: dict[str, Any], fallback_price: float) -> float:
        for key in ("average", "price", "lastTradeTimestamp"):
            value = order.get(key)
            if key == "lastTradeTimestamp":
                continue
            if value is None:
                continue
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
        info = order.get("info")
        if isinstance(info, dict):
            for key in ("average_price", "avg_price", "price", "limit_price"):
                value = info.get(key)
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    continue
                if parsed > 0:
                    return parsed
        return fallback_price

    def _fetch_extended_public_key(self, config: VenueConfig) -> str:
        public_key = str(config.credentials.get("public_key") or "").strip()
        if public_key:
            return public_key

        url = config.endpoints["api_base_url"].rstrip("/") + "/api/v1/user/account/info"
        req = request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": config.user_agent,
                "x-api-key": config.credentials["api_key"],
            },
            method="GET",
        )
        with request.urlopen(req, timeout=10) as response:
            payload = response.read().decode("utf-8")
        import json

        data = json.loads(payload)
        body = data.get("data", data)
        public_key = str(body.get("l2Key") or body.get("l2_key") or "").strip()
        if not public_key:
            raise RuntimeError("Extended account info response did not include a public key")
        return public_key

    def _get_extended_client(self):
        client = self._clients.get("extended")
        if client is not None:
            return client

        config = self.venue_configs["extended"]
        accounts_module = importlib.import_module("x10.perpetual.accounts")
        configuration_module = importlib.import_module("x10.perpetual.configuration")
        trading_client_module = importlib.import_module("x10.perpetual.trading_client")

        public_key = self._fetch_extended_public_key(config)
        stark_account = accounts_module.StarkPerpetualAccount(
            vault=int(config.credentials["account_id"]),
            private_key=config.credentials["private_key"],
            public_key=public_key,
            api_key=config.credentials["api_key"],
        )
        endpoint_config_name = str(
            config.integration.get("endpoint_config", "MAINNET_CONFIG")
        ).strip() or "MAINNET_CONFIG"
        endpoint_config = getattr(configuration_module, endpoint_config_name)
        client = trading_client_module.PerpetualTradingClient(endpoint_config, stark_account)
        self._clients["extended"] = client
        return client

    def _close_extended_client(self, client: Any) -> None:
        close_method = getattr(client, "close", None)
        if close_method is None:
            return
        try:
            if asyncio.iscoroutinefunction(close_method):
                asyncio.run(close_method())
            else:
                close_method()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Extended client close failed: %s", exc)

    def market_order(
        self,
        venue: str,
        symbol: str,
        side: str,
        qty: float,
        expected_quote: Quote,
        fee_bps: float,
    ) -> FillReport | None:
        if venue == "extended":
            return self._market_order_extended(symbol, side, qty, expected_quote, fee_bps)
        if venue == "grvt":
            return self._market_order_grvt(symbol, side, qty, expected_quote, fee_bps)
        raise RuntimeError(f"Unsupported live venue {venue}")

    def _market_order_extended(
        self,
        symbol: str,
        side: str,
        qty: float,
        expected_quote: Quote,
        fee_bps: float,
    ) -> FillReport | None:
        fallback_price = self._quote_price_for_side(side, expected_quote)
        if fallback_price <= 0:
            LOG.warning("Skipping Extended live order because price <= 0 for %s", symbol)
            return None

        client = self._get_extended_client()
        orders_module = importlib.import_module("x10.perpetual.orders")
        order_side = (
            orders_module.OrderSide.BUY
            if side.lower() == "buy"
            else orders_module.OrderSide.SELL
        )
        time_in_force = orders_module.TimeInForce.IOC
        builder_fee = self.venue_configs["extended"].integration.get("builder_fee")

        response = asyncio.run(
            client.place_order(
                market_name=symbol,
                amount_of_synthetic=Decimal(str(qty)),
                price=Decimal(str(fallback_price)),
                side=order_side,
                post_only=False,
                time_in_force=time_in_force,
                reduce_only=False,
                builder_fee=Decimal(str(builder_fee))
                if builder_fee not in (None, "")
                else None,
                external_id=f"arb-bot-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
            )
        )
        if getattr(response, "error", None):
            raise RuntimeError(f"Extended order failed: {response.error}")
        return self._append_live_report(
            "extended",
            symbol,
            side,
            qty,
            fallback_price,
            fee_bps,
            "sdk_ack_only=true",
        )

    def _market_order_grvt(
        self,
        symbol: str,
        side: str,
        qty: float,
        expected_quote: Quote,
        fee_bps: float,
    ) -> FillReport | None:
        fallback_price = self._quote_price_for_side(side, expected_quote)
        if fallback_price <= 0:
            LOG.warning("Skipping GRVT live order because price <= 0 for %s", symbol)
            return None

        client = self._get_grvt_client()
        ccxt_symbol = self._resolve_grvt_symbol(client, symbol)
        order = client.create_order(
            ccxt_symbol,
            "market",
            side.lower(),
            qty,
            None,
            {
                "timeInForce": "IOC",
                "reduceOnly": False,
                "builderFee": bool(
                    self.venue_configs["grvt"].integration.get("builder_fee", False)
                ),
            },
        )
        live_price = self._extract_ccxt_order_price(order, fallback_price)
        return self._append_live_report(
            "grvt",
            symbol,
            side,
            qty,
            live_price,
            fee_bps,
            "ccxt_ack_only=true",
        )

    def close(self) -> None:
        for venue, client in list(self._clients.items()):
            try:
                if venue == "grvt":
                    close_method = getattr(client, "close", None)
                    if callable(close_method):
                        close_method()
                elif venue == "extended":
                    self._close_extended_client(client)
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Client close failed for %s: %s", venue, exc)
        super().close()
