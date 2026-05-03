from typing import Any, Optional


TOKEN_BASE_UNIT = 1_000_000.0


def parse_token_amount(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        parsed = float(value)
        # Data API position sizes are already expressed in whole shares, while
        # some balance endpoints still return fixed-point token base units.
        if abs(parsed) >= TOKEN_BASE_UNIT:
            return parsed / TOKEN_BASE_UNIT
        return parsed
    except (TypeError, ValueError):
        return 0.0


def parse_optional_token_amount(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
        if abs(parsed) >= TOKEN_BASE_UNIT:
            return parsed / TOKEN_BASE_UNIT
        return parsed
    except (TypeError, ValueError):
        return None
