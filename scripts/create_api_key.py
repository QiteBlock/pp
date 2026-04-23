from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
import sys

import yaml
from py_clob_client_v2 import ClobClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


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


def main() -> None:
    load_dotenv()
    with open("config.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    clob = config["clob"]
    private_key_env = clob.get("private_key_env")
    if not private_key_env:
        raise RuntimeError("Missing `clob.private_key_env` in config.yaml")

    private_key = private_key_env if str(private_key_env).startswith("0x") else os.getenv(private_key_env)
    if not private_key:
        raise RuntimeError(
            f"Missing private key env var: {private_key_env}. "
            f"Set `{private_key_env}` in your shell or .env, then rerun this script."
        )

    client = ClobClient(
        host=clob["host"],
        chain_id=int(clob["chain_id"]),
        key=private_key,
        signature_type=clob.get("signature_type", 0),
        funder=clob.get("funder"),
    )

    try:
        creds = client.create_or_derive_api_key()
    except Exception as exc:
        raise RuntimeError(build_rejection_message()) from exc
    if is_error_response(creds):
        raise RuntimeError(f"{build_rejection_message()} API response: {creds}")
    creds_dict = to_dict(creds)
    print(json.dumps(creds_dict, indent=2))
    print()
    print("Export these values into your environment:")
    print(f"{clob['api_key_env']}={creds_dict['apiKey']}")
    print(f"{clob['api_secret_env']}={creds_dict['secret']}")
    print(f"{clob['api_passphrase_env']}={creds_dict['passphrase']}")


def to_dict(creds: Any) -> dict[str, str]:
    if isinstance(creds, dict):
        return creds
    result = {}
    for key in ("apiKey", "secret", "passphrase"):
        if hasattr(creds, key):
            result[key] = getattr(creds, key)
    if result:
        return result
    raise RuntimeError("Unable to normalize API credentials returned by py_clob_client_v2")


def is_error_response(creds: Any) -> bool:
    return isinstance(creds, dict) and ("error" in creds or "status" in creds)


def build_rejection_message() -> str:
    return (
        "Polymarket rejected API key creation. "
        "Most commonly this means `signature_type` and/or `funder` are wrong for your wallet setup. "
        "EOA wallets should use signature_type=0 with funder set to the funded wallet address. "
        "Magic/email wallets should use signature_type=1 with funder set to the Polymarket profile address. "
        "Proxy/Gnosis Safe wallets should use signature_type=2 with funder set to the Polymarket profile address."
    )


if __name__ == "__main__":
    main()
