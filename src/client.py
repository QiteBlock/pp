from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from web3 import Web3
from web3.types import TxReceipt

VENDOR_PATH = Path(__file__).resolve().parents[1] / ".vendor"
if VENDOR_PATH.exists() and str(VENDOR_PATH) not in sys.path:
    sys.path.insert(0, str(VENDOR_PATH))

try:
    from py_clob_client_v2 import ApiCreds, ClobClient, OrderArgs, OrderType, PartialCreateOrderOptions, Side
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams, OpenOrderParams, OrderMarketCancelParams, TradeParams
    from py_clob_client_v2.clob_types import OrderArgsV1, OrderArgsV2
    from py_clob_client_v2.config import get_contract_config
except ImportError:  # pragma: no cover
    ApiCreds = ClobClient = OrderArgs = OrderType = PartialCreateOrderOptions = Side = None
    AssetType = BalanceAllowanceParams = OpenOrderParams = OrderMarketCancelParams = TradeParams = OrderArgsV1 = OrderArgsV2 = None
    get_contract_config = None

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

CTF_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "collateralToken", "type": "address"},
            {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
            {"internalType": "uint256[]", "name": "partition", "type": "uint256[]"},
            {"internalType": "uint256", "name": "amount", "type": "uint256"},
        ],
        "name": "mergePositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


@dataclass
class MarketConfig:
    slug: str
    question: str
    yes_token_id: str
    no_token_id: str
    end_date: Optional[datetime]
    neg_risk: Optional[bool] = None
    condition_id: Optional[str] = None
    reward_rate_per_day: Optional[float] = None
    rewards_min_size: Optional[float] = None
    rewards_max_spread: Optional[float] = None
    market_competitiveness: Optional[float] = None


@dataclass
class OrderBookSnapshot:
    token_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    midpoint: Optional[float]
    spread: Optional[float]
    tick_size: float
    last_trade_price: Optional[float]
    bid_depth_usdc: float = 0.0
    ask_depth_usdc: float = 0.0


class PolymarketClient:
    def __init__(self, config: dict[str, Any]) -> None:
        load_dotenv()
        self.clob_config = config["clob"]
        self.sdk = config["clob"].get("sdk", "v2")
        self.clob_host = config["clob"]["host"]
        self.data_api_host = config.get("data", {}).get("host", "https://data-api.polymarket.com").rstrip("/")
        self.chain_id = int(config["clob"]["chain_id"])
        self.gamma_host = config["gamma"]["host"].rstrip("/")
        self.signature_type = config["clob"].get("signature_type", 0)
        self.funder = config["clob"].get("funder")
        self.rpc_url = config.get("chain", {}).get("rpc_url") or os.getenv("POLYGON_RPC_URL") or "https://polygon-rpc.com"
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
        self.web3 = Web3(Web3.HTTPProvider(self.rpc_url, request_kwargs={"timeout": 20}))
        self.trading_client = self._build_trading_client(self.clob_config)

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
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])
        best_bid = _best_bid(bids)
        best_ask = _best_ask(asks)
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
            bid_depth_usdc=_depth_notional(bids),
            ask_depth_usdc=_depth_notional(asks),
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
        order_type: str = "GTC",
    ) -> dict[str, Any]:
        if dry_run:
            return {
                "status": "dry_run",
                "token_id": token_id,
                "side": side,
                "price": price,
                "size": size,
                "order_type": order_type,
            }
        self.ensure_api_credentials()
        if self.trading_client is None or OrderArgs is None:
            raise RuntimeError("py_clob_client_v2 is not installed.")
        if self.sdk == "v1":
            return self.place_v1_limit_order(token_id, side, price, size, tick_size, dry_run, neg_risk, post_only, order_type)
        order_side = Side.BUY if side.upper() == "BUY" else Side.SELL
        order_args = self.build_order_args(token_id, price, size, order_side)
        clob_order_type = getattr(OrderType, str(order_type).upper(), OrderType.GTC)
        return self.trading_client.create_and_post_order(
            order_args=order_args,
            options=PartialCreateOrderOptions(tick_size=format_tick_size(tick_size), neg_risk=neg_risk),
            order_type=clob_order_type,
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
        order_type: str,
    ) -> dict[str, Any]:
        if dry_run:
            return {
                "status": "dry_run",
                "token_id": token_id,
                "side": side,
                "price": price,
                "size": size,
                "order_type": order_type,
            }
        if self.trading_client is None or V1OrderArgs is None or V1PartialCreateOrderOptions is None:
            raise RuntimeError("py-clob-client is not installed. Run `pip install -r requirements.txt`.")
        order_args = self.build_v1_order_args(token_id, price, size, self.v1_buy_sell(side))
        options = self.v1_order_options(tick_size, neg_risk)
        signed_order = self.trading_client.create_order(order_args, options)
        v1_order_type = getattr(V1OrderType, str(order_type).upper(), V1OrderType.GTC)
        try:
            return self.trading_client.post_order(signed_order, v1_order_type, post_only=post_only)
        except TypeError:
            try:
                return self.trading_client.post_order(signed_order, v1_order_type)
            except Exception as exc:
                if self.try_upgrade_from_v1_order_version_mismatch(exc):
                    return self.place_limit_order(
                        token_id=token_id,
                        side=side,
                        price=price,
                        size=size,
                        tick_size=tick_size,
                        dry_run=dry_run,
                        neg_risk=neg_risk,
                        post_only=post_only,
                        order_type=order_type,
                    )
                raise
        except Exception as exc:
            if self.try_upgrade_from_v1_order_version_mismatch(exc):
                return self.place_limit_order(
                    token_id=token_id,
                    side=side,
                    price=price,
                    size=size,
                    tick_size=tick_size,
                    dry_run=dry_run,
                    neg_risk=neg_risk,
                    post_only=post_only,
                    order_type=order_type,
                )
            raise

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
        params = OpenOrderParams(asset_id=asset_id) if asset_id and OpenOrderParams is not None else None
        return self.trading_client.get_open_orders(params=params)

    def cancel_market_orders(self, asset_id: str, dry_run: bool = True) -> Any:
        if dry_run:
            return {"status": "dry_run", "asset_id": asset_id}
        self.ensure_api_credentials()
        if self.trading_client is None:
            raise RuntimeError("Trading client unavailable.")
        if self.sdk == "v1":
            open_orders = self.get_open_orders(asset_id=asset_id)
            normalized = self._normalize_cancelable_orders(open_orders)
            order_ids = [order_id for order_id in normalized if order_id]
            if not order_ids:
                return []
            return self.trading_client.cancel_orders(order_ids)
        if OrderMarketCancelParams is None:
            raise RuntimeError("OrderMarketCancelParams unavailable in py_clob_client_v2.")
        return self.trading_client.cancel_market_orders(OrderMarketCancelParams(asset_id=asset_id))

    def _normalize_cancelable_orders(self, response: Any) -> list[str]:
        if isinstance(response, list):
            data = response
        elif isinstance(response, dict):
            data = response.get("data") or response.get("orders") or response.get("results") or []
        else:
            data = []
        order_ids: list[str] = []
        for order in data:
            if not isinstance(order, dict):
                continue
            order_id = order.get("id") or order.get("orderId")
            if order_id:
                order_ids.append(str(order_id))
        return order_ids

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

    def fetch_gamma_market_by_slug(self, slug: str) -> Optional[dict[str, Any]]:
        normalized_slug = str(slug or "").strip()
        if not normalized_slug:
            return None
        response = self.session.get(
            f"{self.gamma_host}/markets/slug/{normalized_slug}",
            timeout=30,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, list) and payload:
            first = payload[0]
            return first if isinstance(first, dict) else None
        return None

    def scan_reward_markets(
        self,
        page_size: int = 100,
        next_cursor: Optional[str] = None,
        tag_slugs: Optional[list[str]] = None,
        order_by: str = "competitiveness",
        position: str = "ASC",
        min_volume_24hr: Optional[float] = None,
        max_volume_24hr: Optional[float] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
    ) -> dict[str, Any]:
        params: list[tuple[str, Any]] = [
            ("page_size", min(max(int(page_size), 1), 500)),
            ("order_by", order_by),
            ("position", position),
        ]
        if next_cursor:
            params.append(("next_cursor", next_cursor))
        for tag_slug in tag_slugs or []:
            if tag_slug:
                params.append(("tag_slug", tag_slug))
        if min_volume_24hr is not None:
            params.append(("min_volume_24hr", min_volume_24hr))
        if max_volume_24hr is not None:
            params.append(("max_volume_24hr", max_volume_24hr))
        if min_price is not None:
            params.append(("min_price", min_price))
        if max_price is not None:
            params.append(("max_price", max_price))
        response = self.session.get(
            f"{self.clob_host}/rewards/markets/multi",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected rewards markets payload type: {type(payload).__name__}")
        return payload

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
        aggregated: dict[str, float] = {}
        for item in self.get_user_positions_detailed():
            if not isinstance(item, dict):
                continue
            asset = str(item.get("asset") or "").strip()
            size = _to_float(item.get("size"))
            if not asset or size is None:
                continue
            aggregated[asset] = aggregated.get(asset, 0.0) + size
        return aggregated

    def get_user_positions_detailed(self) -> list[dict[str, Any]]:
        """Fetch detailed user positions from Polymarket Data API."""
        user_address = self._resolve_positions_user_address()
        if not user_address:
            raise RuntimeError(
                "Unable to determine Polymarket profile/proxy wallet address for position lookup. "
                "Set clob.funder in config.yaml to the wallet shown in Polymarket."
            )

        detailed: list[dict[str, Any]] = []
        offset = 0
        limit = 500

        while True:
            response = self.session.get(
                f"{self.data_api_host}/positions",
                params={
                    "user": user_address,
                    "sizeThreshold": 0,
                    "limit": limit,
                    "offset": offset,
                },
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise RuntimeError(f"Unexpected positions payload type: {type(payload).__name__}")

            for item in payload:
                if not isinstance(item, dict):
                    continue
                detailed.append(item)

            if len(payload) < limit:
                break
            offset += limit

        return detailed

    def get_recent_trades(self, asset_id: str, limit: int = 25) -> list[dict[str, Any]]:
        if not asset_id:
            return []
        try:
            response = self.session.get(
                f"{self.data_api_host}/trades",
                params={"market": asset_id, "limit": max(int(limit), 0)},
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, list):
                return payload[: max(int(limit), 0)]
            if isinstance(payload, dict):
                data = payload.get("data") or []
                if isinstance(data, list):
                    return data[: max(int(limit), 0)]
        except Exception:
            return []
        return []

    def merge_position_pair(self, condition_id: str, amount_shares: float, dry_run: bool = True) -> dict[str, Any]:
        if dry_run:
            return {"status": "dry_run", "condition_id": condition_id, "amount_shares": amount_shares}
        if not self.private_key:
            raise RuntimeError("POLY_PRIVATE_KEY is required for onchain merge")
        if get_contract_config is None:
            raise RuntimeError("py_clob_client_v2 contract config unavailable")
        if not self.web3.is_connected():
            raise RuntimeError(f"Polygon RPC unavailable: {self.rpc_url}")
        contract_config = get_contract_config(self.chain_id)
        account = self.web3.eth.account.from_key(self.private_key)
        contract = self.web3.eth.contract(
            address=Web3.to_checksum_address(contract_config.conditional_tokens),
            abi=CTF_ABI,
        )
        amount_base_units = int(round(max(float(amount_shares), 0.0) * 1_000_000))
        if amount_base_units <= 0:
            raise RuntimeError("Merge amount must be positive")
        if amount_base_units > 10**12:
            raise RuntimeError(
                f"Merge amount sanity check failed: {amount_base_units} base units "
                f"for {amount_shares} shares is unexpectedly large"
            )
        condition_bytes = Web3.to_bytes(hexstr=condition_id)
        parent_collection_id = b"\x00" * 32
        gas_price = self.web3.eth.gas_price
        nonce = self.web3.eth.get_transaction_count(account.address, "pending")
        tx_func = contract.functions.mergePositions(
            Web3.to_checksum_address(contract_config.collateral),
            parent_collection_id,
            condition_bytes,
            [1, 2],
            amount_base_units,
        )
        gas_estimate = tx_func.estimate_gas({"from": account.address})
        tx = tx_func.build_transaction(
            {
                "from": account.address,
                "chainId": self.chain_id,
                "nonce": nonce,
                "gas": int(gas_estimate * 1.2),
                "gasPrice": gas_price,
            }
        )
        signed = self.web3.eth.account.sign_transaction(tx, private_key=self.private_key)
        tx_hash = self.web3.eth.send_raw_transaction(signed.raw_transaction)
        receipt: TxReceipt = self.web3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        return {
            "status": "confirmed" if int(receipt.status) == 1 else "reverted",
            "tx_hash": receipt.transactionHash.hex(),
            "gas_used": int(receipt.gasUsed),
            "amount_shares": amount_shares,
            "condition_id": condition_id,
        }

    def _resolve_positions_user_address(self) -> Optional[str]:
        if self.funder:
            return self.funder
        if self.signature_type == 0 and self.private_key:
            try:
                from eth_account import Account  # type: ignore
            except ImportError:
                return None
            return Account.from_key(self.private_key).address
        return None

    def try_upgrade_from_v1_order_version_mismatch(self, exc: Exception) -> bool:
        if self.sdk != "v1":
            return False
        if not is_order_version_mismatch_error(exc):
            return False
        if ClobClient is None:
            return False
        self.sdk = "v2"
        self.api_creds = None
        self.trading_client = self._build_trading_client(self.clob_config)
        if self.trading_client is None:
            self.sdk = "v1"
            self.trading_client = self._build_v1_trading_client(self.clob_config)
            return False
        return True


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
                condition_id=item.get("condition_id"),
                reward_rate_per_day=_to_float(item.get("reward_rate_per_day")),
                rewards_min_size=_to_float(item.get("rewards_min_size")),
                rewards_max_spread=_to_float(item.get("rewards_max_spread")),
                market_competitiveness=_to_float(item.get("market_competitiveness")),
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


def normalize_reward_market(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    tokens = item.get("tokens")
    if not isinstance(tokens, list) or len(tokens) < 2:
        return None
    yes_token_id = ""
    no_token_id = ""
    for token in tokens:
        if not isinstance(token, dict):
            continue
        outcome = str(token.get("outcome") or "").upper()
        token_id = str(token.get("token_id") or "").strip()
        if not token_id:
            continue
        if outcome == "YES" and not yes_token_id:
            yes_token_id = token_id
        elif outcome == "NO" and not no_token_id:
            no_token_id = token_id
    if not yes_token_id or not no_token_id:
        yes_token_id = str(tokens[0].get("token_id") or "").strip()
        no_token_id = str(tokens[1].get("token_id") or "").strip()
    if not yes_token_id or not no_token_id:
        return None
    reward_configs = item.get("rewards_config") or []
    if not isinstance(reward_configs, list):
        reward_configs = []
    rate_per_day = 0.0
    total_rewards = 0.0
    for config in reward_configs:
        if not isinstance(config, dict):
            continue
        rate_per_day += _to_float(config.get("rate_per_day")) or 0.0
        total_rewards += _to_float(config.get("total_rewards")) or 0.0
    first_price = None
    if isinstance(tokens[0], dict):
        first_price = _to_float(tokens[0].get("price"))
    return {
        "slug": item.get("market_slug") or item.get("slug") or item.get("question"),
        "question": item.get("question") or "",
        "category": "",
        "volume_24h": _to_float(item.get("volume_24hr")) or 0.0,
        "end_date": item.get("end_date"),
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "accepting_orders": True,
        "neg_risk": _to_bool(item.get("negative_risk") or item.get("neg_risk")),
        "reference_price": first_price,
        "condition_id": item.get("condition_id"),
        "reward_rate_per_day": rate_per_day,
        "reward_total": total_rewards,
        "rewards_min_size": _to_float(item.get("rewards_min_size")),
        "rewards_max_spread": _to_float(item.get("rewards_max_spread")),
        "market_competitiveness": _to_float(item.get("market_competitiveness")),
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
    text = str(value).strip()
    text = re.sub(r"([+-]\d{2})$", r"\1:00", text)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def is_order_version_mismatch_error(exc: Exception) -> bool:
    return "order_version_mismatch" in str(exc).lower()


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


def _depth_notional(levels: list[dict[str, Any]], max_levels: int = 5) -> float:
    total = 0.0
    if not isinstance(levels, list):
        return total
    for level in levels[: max(max_levels, 0)]:
        if not isinstance(level, dict):
            continue
        price = _to_float(level.get("price"))
        size = _to_float(level.get("size"))
        if price is None or size is None:
            continue
        total += max(price, 0.0) * max(size, 0.0)
    return total


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
