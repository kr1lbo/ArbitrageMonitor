"""
Live smoke-test for OHLCV history on configured exchanges.

Usage:
    python test_ohlcv.py [BASE] [--limit N] [--concurrency N]
"""
from __future__ import annotations

import argparse
import asyncio
import sys

sys.path.insert(0, ".")

import ccxt.async_support as ccxt_a

from core.exchange import EXCHANGE_CONFIGS, make_perp_symbol, make_spot_symbol
from core.history import _async_fetch_ohlcv, exchange_history_options


async def test_one(exc_id: str, mkt: str, base: str, limit: int):
    sym = make_spot_symbol(exc_id, base) if mkt == "spot" else make_perp_symbol(exc_id, base)
    exc = getattr(ccxt_a, exc_id)({
        "enableRateLimit": True,
        "timeout": 30_000,
        "options": exchange_history_options(exc_id, mkt),
    })
    try:
        data = await _async_fetch_ohlcv(exc, sym, "1m", limit)
        return {
            "status": "OK" if len(data) >= limit else "PARTIAL",
            "count": len(data),
            "symbol": sym,
            "error": None,
        }
    except ValueError as exc_msg:
        return {
            "status": "SKIP",
            "count": 0,
            "symbol": sym,
            "error": str(exc_msg),
        }
    except Exception as exc_msg:
        return {
            "status": "FAIL",
            "count": 0,
            "symbol": sym,
            "error": str(exc_msg),
        }
    finally:
        await exc.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("base", nargs="?", default="BTC")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()

    pairs = []
    for exc_id, cfg in EXCHANGE_CONFIGS.items():
        if cfg.get("spot"):
            pairs.append((exc_id, "spot"))
        if cfg.get("perp"):
            pairs.append((exc_id, "perp"))

    sem = asyncio.Semaphore(max(1, args.concurrency))

    async def guarded(exc_id: str, mkt: str):
        async with sem:
            return await test_one(exc_id, mkt, args.base.upper(), args.limit)

    print(f"Testing OHLCV for {args.base.upper()}")
    print("=" * 72)

    results = await asyncio.gather(*(guarded(exc_id, mkt) for exc_id, mkt in pairs))

    ok = partial = skip = fail = 0
    for (exc_id, mkt), res in zip(pairs, results):
        label = f"{exc_id:20s} {mkt:4s} {res['symbol']:18s}"
        if res["status"] == "OK":
            print(f"[OK   ] {label}  {res['count']} bars")
            ok += 1
        elif res["status"] == "PARTIAL":
            print(f"[PART ] {label}  {res['count']} bars")
            partial += 1
        elif res["status"] == "SKIP":
            print(f"[SKIP ] {label}  {res['error'][:100]}")
            skip += 1
        else:
            print(f"[FAIL ] {label}  {res['error'][:120]}")
            fail += 1

    print("=" * 72)
    print(f"OK: {ok}  PARTIAL: {partial}  SKIP: {skip}  FAIL: {fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
