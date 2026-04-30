"""
Live smoke-test for funding history on configured perpetual exchanges.

Usage:
    python test_funding.py [BASE] [--limit N]
"""
from __future__ import annotations

import argparse
import asyncio
import sys

sys.path.insert(0, ".")

import ccxt

from core.exchange import EXCHANGE_CONFIGS, make_perp_symbol
from core.history import fetch_funding_history, funding_interval_label


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base", nargs="?", default="BTC")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    base = args.base.upper()
    exchanges = [exc_id for exc_id, cfg in EXCHANGE_CONFIGS.items() if cfg.get("funding")]

    print(f"Testing funding history for {base}")
    print("=" * 72)

    ok = skip = fail = 0
    for exc_id in exchanges:
        symbol = make_perp_symbol(exc_id, base)
        try:
            has_history = getattr(ccxt, exc_id)().has.get("fetchFundingRateHistory")
            rows = await fetch_funding_history(exc_id, "perp", base, args.limit)
            label = f"{exc_id:20s} {symbol:18s}"
            if rows:
                first = rows[0]["rate"]
                last = rows[-1]["rate"]
                interval = funding_interval_label(rows)
                print(
                    f"[OK   ] {label}  {len(rows)} rows  interval={interval:>3s}  "
                    f"first={first:+.5f}%  last={last:+.5f}%"
                )
                ok += 1
            elif has_history:
                print(f"[SKIP ] {label}  no rows returned")
                skip += 1
            else:
                print(f"[SKIP ] {label}  ccxt has no fetchFundingRateHistory")
                skip += 1
        except Exception as exc:
            print(f"[FAIL ] {exc_id:20s} {symbol:18s}  {str(exc)[:120]}")
            fail += 1

    print("=" * 72)
    print(f"OK: {ok}  SKIP: {skip}  FAIL: {fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
