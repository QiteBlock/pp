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
    print(f"private_key_loaded={bool(client.private_key)}")
    print(f"api_creds_loaded={bool(client.api_creds)}")
    try:
        response = client.validate_auth()
    except Exception as exc:
        print("clob_auth=failed")
        print(f"error={exc}")
        raise SystemExit(1) from exc
    print("clob_auth=ok")
    print(response)


if __name__ == "__main__":
    main()
