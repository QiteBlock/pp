from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
VENDOR = ROOT / ".vendor"
if VENDOR.exists() and str(VENDOR) not in sys.path:
    sys.path.insert(0, str(VENDOR))

from py_clob_client_v2 import ApiCreds, ClobClient
try:
    from py_clob_client.client import ClobClient as V1ClobClient
except ImportError:  # pragma: no cover
    V1ClobClient = None

from src.client import PolymarketClient, load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Try deriving CLOB API credentials from L1 auth and nonce.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=20)
    parser.add_argument("--signature-type", type=int)
    parser.add_argument("--funder")
    parser.add_argument("--sdk", choices=("v1", "v2"))
    parser.add_argument("--write-env", action="store_true", help="Write the derived credentials into .env without printing secrets.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv()
    with open(args.config, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    if args.signature_type is not None:
        config["clob"]["signature_type"] = args.signature_type
    if args.funder is not None:
        config["clob"]["funder"] = args.funder
    if args.sdk is not None:
        config["clob"]["sdk"] = args.sdk

    base_client = PolymarketClient(config)
    if not base_client.private_key:
        raise RuntimeError("POLY_PRIVATE_KEY is missing")

    for nonce in range(args.start, args.end + 1):
        try:
            creds = derive_for_nonce(config, base_client.private_key, nonce)
            if validates(config, base_client.private_key, creds):
                if args.write_env:
                    write_env_creds(config, creds)
                    print(f"derived_nonce={nonce}")
                    print("wrote_env=true")
                    return
                print(f"derived_nonce={nonce}")
                print(f"POLY_CLOB_API_KEY={creds.api_key}")
                print(f"POLY_CLOB_SECRET={creds.api_secret}")
                print(f"POLY_CLOB_PASS_PHRASE={creds.api_passphrase}")
                return
            print(f"nonce={nonce} derived_but_validation_failed")
        except Exception as exc:
            print(f"nonce={nonce} failed: {exc}")
    raise SystemExit("No valid CLOB credentials derived in nonce range")


def derive_for_nonce(config: dict, private_key: str, nonce: int) -> ApiCreds:
    clob = config["clob"]
    if clob.get("sdk") == "v1":
        if V1ClobClient is None:
            raise RuntimeError("py-clob-client is not installed")
        client = V1ClobClient(
            clob["host"],
            chain_id=int(clob["chain_id"]),
            key=private_key,
            signature_type=clob.get("signature_type"),
            funder=clob.get("funder"),
        )
        return client.derive_api_key(nonce=nonce)
    client = ClobClient(
        host=clob["host"],
        chain_id=int(clob["chain_id"]),
        key=private_key,
        signature_type=clob.get("signature_type"),
        funder=clob.get("funder"),
    )
    return client.derive_api_key(nonce=nonce)


def validates(config: dict, private_key: str, creds: ApiCreds) -> bool:
    clob = config["clob"]
    if clob.get("sdk") == "v1":
        if V1ClobClient is None:
            return False
        client = V1ClobClient(
            clob["host"],
            chain_id=int(clob["chain_id"]),
            key=private_key,
            signature_type=clob.get("signature_type"),
            funder=clob.get("funder"),
        )
        client.set_api_creds(creds)
        try:
            client.get_api_keys()
        except Exception:
            return False
        return True
    client = ClobClient(
        host=clob["host"],
        chain_id=int(clob["chain_id"]),
        key=private_key,
        creds=creds,
        signature_type=clob.get("signature_type"),
        funder=clob.get("funder"),
    )
    try:
        client.get_api_keys()
    except Exception:
        return False
    return True


def write_env_creds(config: dict, creds: ApiCreds, dotenv_path: str = ".env") -> None:
    clob = config["clob"]
    replacements = {
        clob["api_key_env"]: creds.api_key,
        clob["api_secret_env"]: creds.api_secret,
        clob["api_passphrase_env"]: creds.api_passphrase,
    }
    path = Path(dotenv_path)
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    new_lines: list[str] = []
    for line in existing_lines:
        if "=" not in line or line.strip().startswith("#"):
            new_lines.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in replacements:
            new_lines.append(f"{key}={replacements[key]}")
            seen.add(key)
        else:
            new_lines.append(line)
    for key, value in replacements.items():
        if key not in seen:
            new_lines.append(f"{key}={value}")
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
