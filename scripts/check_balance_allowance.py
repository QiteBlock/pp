from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.client import PolymarketClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check CLOB collateral balance and allowance.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--signature-type", type=int)
    parser.add_argument("--funder")
    parser.add_argument("--update", action="store_true", help="Ask CLOB to refresh balance/allowance before reading it.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with open(args.config, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if args.signature_type is not None:
        config["clob"]["signature_type"] = args.signature_type
    if args.funder is not None:
        config["clob"]["funder"] = args.funder
    client = PolymarketClient(config)
    print(f"signature_type={config['clob'].get('signature_type')}")
    print(f"funder={config['clob'].get('funder')}")
    if args.update:
        print("update_response=", client.update_collateral_balance_allowance())
    print("balance_allowance=", client.get_collateral_balance_allowance())


if __name__ == "__main__":
    main()
