import argparse
import asyncio

import ccxt.async_support as ccxt_a
import ccxt.pro as ccxt_pro

from core.config import ccxt_config, format_network_error, proxy_mode_label
from core.history import exchange_history_options


async def _rest_check(exchange_id: str, symbol: str, timeout_ms: int) -> bool:
    exc = getattr(ccxt_a, exchange_id)(
        ccxt_config(exchange_history_options(exchange_id, "spot"), timeout=timeout_ms, proxy_kind="rest")
    )
    try:
        await exc.load_markets()
        ticker = await exc.fetch_ticker(symbol)
        ok = bool(ticker.get("bid") or ticker.get("ask") or ticker.get("last"))
        print(f"REST {exchange_id} {symbol}: {'OK' if ok else 'EMPTY'}")
        return ok
    except Exception as exc_err:
        print(format_network_error(f"{exchange_id}_spot", "REST load_markets/fetch_ticker", exc_err))
        return False
    finally:
        await exc.close()


async def _ws_check(exchange_id: str, symbol: str, timeout_ms: int) -> bool:
    exc = getattr(ccxt_pro, exchange_id)(
        ccxt_config(exchange_history_options(exchange_id, "spot"), timeout=timeout_ms, proxy_kind="websocket")
    )
    try:
        await exc.load_markets()
        ticker = await asyncio.wait_for(exc.watch_ticker(symbol), timeout=timeout_ms / 1000)
        ok = bool(ticker.get("bid") or ticker.get("ask") or ticker.get("last"))
        print(f"WS   {exchange_id} {symbol}: {'OK' if ok else 'EMPTY'}")
        return ok
    except Exception as exc_err:
        print(format_network_error(f"{exchange_id}_spot", "WS watch_ticker", exc_err))
        return False
    finally:
        await exc.close()


async def main():
    parser = argparse.ArgumentParser(description="Проверка proxy из config.json для REST и WebSocket.")
    parser.add_argument("--exchange", default="bybit")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--timeout", type=int, default=15000, help="Таймаут в мс")
    args = parser.parse_args()

    print(proxy_mode_label("rest"))
    print(proxy_mode_label("websocket"))
    rest_ok = await _rest_check(args.exchange, args.symbol, args.timeout)
    ws_ok = await _ws_check(args.exchange, args.symbol, args.timeout) if rest_ok else False
    raise SystemExit(0 if rest_ok and ws_ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
