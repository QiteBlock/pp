from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VENDOR_PATH = Path(__file__).resolve().parents[1] / ".vendor"
if VENDOR_PATH.exists() and str(VENDOR_PATH) not in sys.path:
    sys.path.insert(0, str(VENDOR_PATH))

try:
    from py_clob_client_v2 import ApiCreds, ClobClient, OrderArgs, OrderType, PartialCreateOrderOptions, Side
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
    from py_clob_client_v2.clob_types import OrderArgsV1, OrderArgsV2
except ImportError:  # pragma: no cover
    ApiCreds = ClobClient = OrderArgs = OrderType = PartialCreateOrderOptions = Side = None
    AssetType = BalanceAllowanceParams = OrderArgsV1 = OrderArgsV2 = None

try:
    from py_clob_client.client import ClobClient as V1ClobClient
    from py_clob_client.clob_types import ApiCreds as V1ApiCreds
    from py_clob_client.clob_types import AssetType as V1AssetType
    from py_clob_client.clob_types import BalanceAllowanceParams as V1BalanceAllowanceParams
    from py_clob_client.clob_types import OrderArgs as V1OrderArgs
    from py_clob_client.clob_types import OrderType as V1OrderType
    from py_clob_client.clob_types import OpenOrderParams as V1OpenOrderParams
    from py_clob_client.clob_types import PartialCreateOrderOptions as V1PartialCreateOrderOptions
    from py_clob_client.order_builder.constants import BUY as V1_BUY
    from py_clob_client.order_builder.constants import SELL as V1_SELL
except ImportError:  # pragma: no cover
    V1ClobClient = V1ApiCreds = V1AssetType = V1BalanceAllowanceParams = V1OrderArgs = V1OrderType = V1OpenOrderParams = V1PartialCreateOrderOptions = None
    V1_BUY = V1_SELL = None


@dataclass
class MarketConfig:
    slug: str
    question: str
    yes_token_id: str
    no_token_id: str
    end_date: Optional[datetime]
    neg_risk: Optional[bool] = None


@dataclass
class OrderBookSnapshot:
    token_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    midpoint: Optional[float]
    spread: Optional[float]
    tick_size: float
    last_trade_price: Optional[float]


class PolymarketClient:
    def __init__(self, config: dict[str, Any]) -> None:
        load_dotenv()
        self.sdk = config["clob"].get("sdk", "v2")
        self.clob_host = config["clob"]["host"]
        self.chain_id = int(config["clob"]["chain_id"])
        self.gamma_host = config["gamma"]["host"].rstrip("/")
        self.signature_type = config["clob"].get("signature_type", 0)
        self.funder = config["clob"].get("funder")
        self.private_key: Optional[str] = None
        self.api_creds: Any = None
        self.session = requests.Session()
        retries = Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.trading_client = self._build_trading_client(config["clob"])

    def _build_trading_client(self, clob_config: dict[str, Any]) -> Any:
        if self.sdk == "v1":
            return self._build_v1_trading_client(clob_config)
        if ClobClient is None:
            return None
        self.private_key = os.getenv(clob_config["private_key_env"])
        api_key = os.getenv(clob_config["api_key_env"])
        api_secret = os.getenv(clob_config["api_secret_env"])
        api_passphrase = os.getenv(clob_config["api_passphrase_env"])
        if not self.private_key:
            return None
        self.api_creds = None
        if api_key and api_secret and api_passphrase:
            self.api_creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        return ClobClient(
            host=self.clob_host,
            chain_id=self.chain_id,
            key=self.private_key,
            creds=self.api_creds,
            signature_type=self.signature_type,
            funder=self.funder,
        )

    def _build_v1_trading_client(self, clob_config: dict[str, Any]) -> Any:
        if V1ClobClient is None:
            return None
        self.private_key = os.getenv(clob_config["private_key_env"])
        api_key = os.getenv(clob_config["api_key_env"])
        api_secret = os.getenv(clob_config["api_secret_env"])
        api_passphrase = os.getenv(clob_config["api_passphrase_env"])
        if not self.private_key:
            return None
        self.api_creds = None
        if api_key and api_secret and api_passphrase:
            self.api_creds = V1ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        client = V1ClobClient(
            self.clob_host,
            key=self.private_key,
            chain_id=self.chain_id,
            signature_type=self.signature_type,
            funder=self.funder,
        )
        if self.api_creds is not None:
            client.set_api_creds(self.api_creds)
        return client

    def ensure_api_credentials(self) -> Any:
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable. Install dependencies and set wallet/API env vars first.")
        if self.api_creds is not None:
            if hasattr(self.trading_client, "set_api_creds"):
                self.trading_client.set_api_creds(self.api_creds)
            return self.api_creds
        if self.sdk == "v1" and hasattr(self.trading_client, "create_or_derive_api_creds"):
            creds = self.trading_client.create_or_derive_api_creds()
        else:
            creds = self.trading_client.create_or_derive_api_key()
        if isinstance(creds, dict) and "error" in creds:
            raise RuntimeError(f"Polymarket rejected API key creation: {creds['error']}")
        self.api_creds = creds
        if hasattr(self.trading_client, "set_api_creds"):
            self.trading_client.set_api_creds(creds)
            return creds
        self.trading_client = ClobClient(
            host=self.clob_host,
            chain_id=self.chain_id,
            key=self.private_key,
            creds=creds,
            signature_type=self.signature_type,
            funder=self.funder,
        )
        return creds

    def get_orderbook(self, token_id: str) -> OrderBookSnapshot:
        response = self.session.get(f"{self.clob_host}/book", params={"token_id": token_id}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        best_bid = _best_bid(payload.get("bids", []))
        best_ask = _best_ask(payload.get("asks", []))
        midpoint = ((best_bid + best_ask) / 2.0) if best_bid is not None and best_ask is not None else None
        spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
        return OrderBookSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            midpoint=midpoint,
            spread=spread,
            tick_size=float(payload.get("tick_size", 0.01) or 0.01),
            last_trade_price=_to_float(payload.get("last_trade_price")),
        )

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        tick_size: float,
        dry_run: bool = True,
        neg_risk: Optional[bool] = None,
        post_only: bool = True,
    ) -> dict[str, Any]:
        if dry_run:
            return {"status": "dry_run", "token_id": token_id, "side": side, "price": price, "size": size}
        self.ensure_api_credentials()
        if self.trading_client is None or OrderArgs is None:
            raise RuntimeError("py_clob_client_v2 is not installed.")
        if self.sdk == "v1":
            return self.place_v1_limit_order(token_id, side, price, size, tick_size, dry_run, neg_risk, post_only)
        order_side = Side.BUY if side.upper() == "BUY" else Side.SELL
        order_args = self.build_order_args(token_id, price, size, order_side)
        return self.trading_client.create_and_post_order(
            order_args=order_args,
            options=PartialCreateOrderOptions(tick_size=format_tick_size(tick_size), neg_risk=neg_risk),
            order_type=OrderType.GTC,
            post_only=post_only,
        )

    def build_order_args(self, token_id: str, price: float, size: float, order_side: Any) -> Any:
        if OrderArgsV2 is not None:
            return OrderArgsV2(token_id=token_id, price=price, size=size, side=order_side)
        return OrderArgs(token_id=token_id, price=price, size=size, side=order_side)

    def place_v1_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        tick_size: float,
        dry_run: bool,
        neg_risk: Optional[bool],
        post_only: bool,
    ) -> dict[str, Any]:
        if dry_run:
            return {"status": "dry_run", "token_id": token_id, "side": side, "price": price, "size": size}
        if self.trading_client is None or V1OrderArgs is None or V1PartialCreateOrderOptions is None:
            raise RuntimeError("py-clob-client is not installed. Run `pip install -r requirements.txt`.")
        order_args = self.build_v1_order_args(token_id, price, size, self.v1_buy_sell(side))
        options = self.v1_order_options(tick_size, neg_risk)
        signed_order = self.trading_client.create_order(order_args, options)
        try:
            return self.trading_client.post_order(signed_order, V1OrderType.GTC, post_only=post_only)
        except TypeError:
            return self.trading_client.post_order(signed_order, V1OrderType.GTC)

    def v1_buy_sell(self, side: str) -> Any:
        return V1_BUY if side.upper() == "BUY" else V1_SELL

    def build_v1_order_args(self, token_id: str, price: float, size: float, order_side: Any) -> Any:
        return V1OrderArgs(token_id=token_id, price=price, size=size, side=order_side)

    def v1_order_options(self, tick_size: float, neg_risk: Optional[bool]) -> Any:
        return V1PartialCreateOrderOptions(tick_size=format_tick_size(tick_size), neg_risk=neg_risk)

    def cancel_all_orders(self, dry_run: bool = True) -> Any:
        if dry_run:
            return {"status": "dry_run"}
        self.ensure_api_credentials()
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable.")
        return self.trading_client.cancel_all()

    def validate_auth(self) -> Any:
        self.ensure_api_credentials()
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable.")
        return self.trading_client.get_api_keys()

    def get_collateral_balance_allowance(self) -> dict[str, Any]:
        self.ensure_api_credentials()
        if self.trading_client is None or BalanceAllowanceParams is None or AssetType is None:
            raise RuntimeError("Trading client unavailable.")
        if self.sdk == "v1":
            return self.trading_client.get_balance_allowance(
                V1BalanceAllowanceParams(asset_type=V1AssetType.COLLATERAL)
            )
        return self.trading_client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )

    def get_conditional_balance_allowance(self, token_id: str) -> dict[str, Any]:
        self.ensure_api_credentials()
        if self.trading_client is None or BalanceAllowanceParams is None or AssetType is None:
            raise RuntimeError("Trading client unavailable.")
        if self.sdk == "v1":
            return self.trading_client.get_balance_allowance(
                V1BalanceAllowanceParams(asset_type=V1AssetType.CONDITIONAL, token_id=token_id)
            )
        return self.trading_client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )

    def get_neg_risk(self, token_id: str) -> bool:
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable.")
        return bool(self.trading_client.get_neg_risk(token_id))

    def update_collateral_balance_allowance(self) -> dict[str, Any]:
        self.ensure_api_credentials()
        if self.trading_client is None or BalanceAllowanceParams is None or AssetType is None:
            raise RuntimeError("Trading client unavailable.")
        if self.sdk == "v1":
            return self.trading_client.update_balance_allowance(
                V1BalanceAllowanceParams(asset_type=V1AssetType.COLLATERAL)
            )
        return self.trading_client.update_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )

    def get_open_orders(self, asset_id: Optional[str] = None) -> Any:
        self.ensure_api_credentials()
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable.")
        if self.sdk == "v1":
            params = V1OpenOrderParams(asset_id=asset_id) if asset_id else None
            return self.trading_client.get_orders(params)
        return self.trading_client.get_open_orders()

    def check_geoblock(self) -> dict[str, Any]:
        response = self.session.get("https://polymarket.com/api/geoblock", timeout=15)
        response.raise_for_status()
        return response.json()

    def scan_markets(self, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.gamma_host}/markets",
            params={"active": "true", "closed": "false", "limit": limit, "offset": offset},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def fetch_market_history(self, token_id: str, interval: str = "1d", fidelity: int = 60) -> list[float]:
        response = self.session.get(
            f"{self.clob_host}/prices-history",
            params={"market": token_id, "interval": interval, "fidelity": fidelity},
            timeout=20,
        )
        if response.status_code >= 400:
            return []
        payload = response.json()
        history = payload.get("history", payload if isinstance(payload, list) else [])
        return [_to_float(point.get("p")) for point in history if _to_float(point.get("p")) is not None]

    def get_user_fills(self, since_ts: Optional[int] = None) -> list[dict[str, Any]]:
        """Fetch fills for the authenticated user since a given timestamp (milliseconds).
        If since_ts is None, fetches recent fills from the API (typically last 24-48 hours).
        Returns list of fill records with structure: {id, token_id, symbol, price, size, side, timestamp, ...}
        """
        self.ensure_api_credentials()
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable.")
        try:
            if self.sdk == "v1":
                if hasattr(self.trading_client, "get_fills"):
                    return self.trading_client.get_fills()
                if hasattr(self.trading_client, "get_trades"):
                    return self.trading_client.get_trades()
            else:
                if hasattr(self.trading_client, "get_fills"):
                    params = {"since_timestamp": since_ts} if since_ts is not None else {}
                    return self.trading_client.get_fills(**params)
                if hasattr(self.trading_client, "get_trades"):
                    return self.trading_client.get_trades()
        except Exception as exc:
            print(f"Warning: Failed to fetch fills from trading client: {exc}")
        return []

    def get_user_positions(self) -> dict[str, Any]:
        """Fetch user's current positions (balances of each outcome token).
        Returns dict mapping token_id to balance amount.
        """
        self.ensure_api_credentials()
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable.")
        try:
            if self.sdk == "v1":
                if hasattr(self.trading_client, "get_balances"):
                    return self.trading_client.get_balances()
                if hasattr(self.trading_client, "get_positions"):
                    return self.trading_client.get_positions()
            else:
                if hasattr(self.trading_client, "get_user_balances"):
                    return self.trading_client.get_user_balances()
                if hasattr(self.trading_client, "get_positions"):
                    return self.trading_client.get_positions()
        except Exception as exc:
            print(f"Warning: Failed to fetch positions from trading client: {exc}")
        return {}


def parse_market_configs(raw_markets: list[dict[str, Any]]) -> list[MarketConfig]:
    result: list[MarketConfig] = []
    for item in raw_markets:
        result.append(
            MarketConfig(
                slug=item["slug"],
                question=item.get("question", item["slug"]),
                yes_token_id=item["yes_token_id"],
                no_token_id=item["no_token_id"],
                end_date=_parse_datetime(item.get("end_date")),
                neg_risk=item.get("neg_risk"),
            )
        )
    return result


def normalize_gamma_market(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    token_ids = item.get("clobTokenIds")
    parsed_token_ids = _parse_token_ids(token_ids)
    if len(parsed_token_ids) < 2:
        return None
    return {
        "slug": item.get("slug") or item.get("questionID") or item.get("id"),
        "question": item.get("question") or "",
        "category": (item.get("category") or "").lower(),
        "volume_24h": _to_float(item.get("volume24hrClob")) or _to_float(item.get("volume24hr")) or 0.0,
        "end_date": item.get("endDate") or item.get("endDateIso") or item.get("umaEndDateIso"),
        "yes_token_id": parsed_token_ids[0],
        "no_token_id": parsed_token_ids[1],
        "accepting_orders": bool(item.get("acceptingOrders", True)),
        "neg_risk": _to_bool(item.get("negRisk") or item.get("neg_risk")),
        "reference_price": _extract_reference_price(item),
    }


def _parse_token_ids(token_ids: Any) -> list[str]:
    if isinstance(token_ids, list):
        return [str(value) for value in token_ids]
    if isinstance(token_ids, str):
        stripped = token_ids.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return [str(value) for value in parsed]
            except json.JSONDecodeError:
                return []
        return [part.strip() for part in stripped.split(",") if part.strip()]
    return []


def _extract_reference_price(item: dict[str, Any]) -> Optional[float]:
    direct_candidates = (
        item.get("lastTradePrice"),
        item.get("last_trade_price"),
        item.get("price"),
        item.get("yesPrice"),
        item.get("yes_price"),
    )
    for candidate in direct_candidates:
        parsed = _to_float(candidate)
        if parsed is not None:
            return parsed

    outcome_prices = item.get("outcomePrices") or item.get("outcome_prices")
    if isinstance(outcome_prices, list) and outcome_prices:
        return _to_float(outcome_prices[0])
    if isinstance(outcome_prices, str):
        stripped = outcome_prices.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list) and parsed:
                    return _to_float(parsed[0])
            except json.JSONDecodeError:
                return None
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _best_bid(levels: list[dict[str, Any]]) -> Optional[float]:
    prices = [_to_float(level.get("price")) for level in levels]
    valid_prices = [price for price in prices if price is not None]
    return max(valid_prices) if valid_prices else None


def _best_ask(levels: list[dict[str, Any]]) -> Optional[float]:
    prices = [_to_float(level.get("price")) for level in levels]
    valid_prices = [price for price in prices if price is not None]
    return min(valid_prices) if valid_prices else None


def load_dotenv(dotenv_path: str = ".env") -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def format_tick_size(tick_size: float) -> str:
    allowed = ("0.1", "0.01", "0.001", "0.0001")
    closest = min(allowed, key=lambda value: abs(float(value) - tick_size))
    return closest
