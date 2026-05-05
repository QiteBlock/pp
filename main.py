from __future__ import annotations

import sys

from src.market_maker import run_market_maker


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    run_market_maker(config_path)
