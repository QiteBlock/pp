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
    geo = client.check_geoblock()
    print(f"blocked={geo.get('blocked')}")
    print(f"country={geo.get('country')}")
    print(f"region={geo.get('region')}")


if __name__ == "__main__":
    main()
