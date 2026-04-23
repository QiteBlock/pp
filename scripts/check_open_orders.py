from __future__ import annotations

from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.client import PolymarketClient


def main() -> None:
    with open("config.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    client = PolymarketClient(config)
    response = client.get_open_orders()
    orders = response if isinstance(response, list) else response.get("data", response)
    print(orders)


if __name__ == "__main__":
    main()
