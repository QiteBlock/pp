from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from .analytics import FillRecord
from .inventory import MarketInventory


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class BotControlState:
    trading_enabled: bool = True
    updated_at: str = ""
    source: str = "startup"


class BotControl:
    def __init__(self, state_dir: str = "data") -> None:
        self.state_path = Path(state_dir) / "bot_state.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load()

    @property
    def trading_enabled(self) -> bool:
        return self.state.trading_enabled

    def enable(self, source: str) -> None:
        self.state.trading_enabled = True
        self.state.updated_at = _utc_now()
        self.state.source = source
        self._save()

    def disable(self, source: str) -> None:
        self.state.trading_enabled = False
        self.state.updated_at = _utc_now()
        self.state.source = source
        self._save()

    def _load(self) -> BotControlState:
        if not self.state_path.exists():
            state = BotControlState(updated_at=_utc_now())
            self._write_state(state)
            return state
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            return BotControlState(**payload)
        except Exception:
            state = BotControlState(updated_at=_utc_now(), source="recovered_default")
            self._write_state(state)
            return state

    def _save(self) -> None:
        self._write_state(self.state)

    def _write_state(self, state: BotControlState) -> None:
        self.state_path.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")


@dataclass
class TelegramRuntimeState:
    next_update_id: int = 0


class TelegramController:
    def __init__(self, config: dict[str, Any], state_dir: str = "data") -> None:
        self.enabled = bool(config.get("enabled", False))
        self.bot_token_env = config.get("bot_token_env", "TELEGRAM_BOT_TOKEN")
        self.chat_id_env = config.get("chat_id_env", "TELEGRAM_CHAT_ID")
        self.poll_timeout_seconds = int(config.get("poll_timeout_seconds", 0))
        self.notify_fills_enabled = bool(config.get("notify_fills", True))
        self.notify_market_switch_enabled = bool(config.get("notify_market_switch", True))
        self.token = os.getenv(self.bot_token_env, "").strip()
        self.chat_id = os.getenv(self.chat_id_env, "").strip()
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else ""
        self.state_path = Path(state_dir) / "telegram_state.json"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
        self.session = requests.Session()
        if self.enabled and (not self.token or not self.chat_id):
            print(
                "Telegram disabled: missing bot token or chat id. "
                f"Expected env vars {self.bot_token_env} and {self.chat_id_env}."
            )
            self.enabled = False

    def is_enabled(self) -> bool:
        return self.enabled

    def poll_commands(self) -> list[str]:
        if not self.enabled:
            return []
        try:
            response = self.session.get(
                f"{self.base_url}/getUpdates",
                params={"offset": self.state.next_update_id, "timeout": self.poll_timeout_seconds},
                timeout=max(self.poll_timeout_seconds + 5, 10),
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            print(f"Telegram polling failed: {exc}")
            return []
        if not payload.get("ok"):
            print(f"Telegram polling failed: {payload}")
            return []
        commands: list[str] = []
        for update in payload.get("result", []):
            update_id = int(update.get("update_id", 0))
            self.state.next_update_id = max(self.state.next_update_id, update_id + 1)
            message = update.get("message") or update.get("edited_message") or {}
            chat_id = str((message.get("chat") or {}).get("id") or "")
            if chat_id != self.chat_id:
                continue
            text = (message.get("text") or "").strip()
            command = _parse_command(text)
            if command:
                commands.append(command)
        self._save_state()
        return commands

    def send_message(self, text: str) -> bool:
        if not self.enabled:
            return False
        try:
            response = self.session.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": self.chat_id, "text": text},
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            print(f"Telegram send failed: {exc}")
            return False
        if not payload.get("ok"):
            print(f"Telegram send failed: {payload}")
            return False
        return True

    def notify_fill(self, record: FillRecord, inventory: MarketInventory) -> None:
        if not self.enabled or not self.notify_fills_enabled:
            return
        message = (
            "Fill detected\n"
            f"market={record.market}\n"
            f"side={record.side} {record.outcome}\n"
            f"size={record.size:.4f} price={record.price:.4f}\n"
            f"inventory_yes={inventory.yes_shares:.4f} inventory_no={inventory.no_shares:.4f}\n"
            f"net_delta={inventory.net_delta:.4f}"
        )
        self.send_message(message)

    def notify_market_switch(self, previous_market: Optional[str], new_market: str, question: str) -> None:
        if not self.enabled or not self.notify_market_switch_enabled:
            return
        previous_text = previous_market or "none"
        self.send_message(
            "Market switched\n"
            f"from={previous_text}\n"
            f"to={new_market}\n"
            f"question={question}"
        )

    def _load_state(self) -> TelegramRuntimeState:
        if not self.state_path.exists():
            return TelegramRuntimeState()
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            return TelegramRuntimeState(**payload)
        except Exception:
            return TelegramRuntimeState()

    def _save_state(self) -> None:
        self.state_path.write_text(json.dumps(asdict(self.state), indent=2), encoding="utf-8")


def _parse_command(text: str) -> Optional[str]:
    if not text.startswith("/"):
        return None
    command = text.split()[0].split("@")[0].lower()
    if command in {"/start", "/stop", "/status", "/help"}:
        return command[1:]
    return None
