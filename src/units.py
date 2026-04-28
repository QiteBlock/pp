from typing import Any, Optional


TOKEN_BASE_UNIT = 1_000_000.0


def parse_token_amount(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value) / TOKEN_BASE_UNIT
    except (TypeError, ValueError):
        return 0.0


def parse_optional_token_amount(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value) / TOKEN_BASE_UNIT
    except (TypeError, ValueError):
        return None
