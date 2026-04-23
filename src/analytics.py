from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class FillRecord:
    timestamp: str
    market: str
    outcome: str
    side: str
    size: float
    price: float
    fair_value: float | None = None
    spread_capture: float | None = None


class AnalyticsWriter:
    def __init__(self, log_dir: str) -> None:
        self.base_path = Path(log_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.fills_csv = self.base_path / "fills.csv"
        self.events_jsonl = self.base_path / "events.jsonl"
        self._ensure_fill_header()

    def _ensure_fill_header(self) -> None:
        if self.fills_csv.exists():
            return
        with self.fills_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(FillRecord.__annotations__.keys()))
            writer.writeheader()

    def log_fill(self, record: FillRecord) -> None:
        with self.fills_csv.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(FillRecord.__annotations__.keys()))
            writer.writerow(asdict(record))

    def log_event(self, event_type: str, payload: dict[str, Any]) -> None:
        message = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "payload": payload,
        }
        with self.events_jsonl.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(message) + "\n")
