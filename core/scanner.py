from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from statistics import median
from typing import Any

from core.exchange import EXCHANGE_CONFIGS, LOW_LIQUIDITY_VOLUME_24H, source_label
from core.config import async_retry, ccxt_config, format_network_error
from core.history import exchange_history_options


QUOTE_CODES = {"USDT", "USDC"}
MAX_EXCHANGE_CONCURRENCY = 4
PRICE_OUTLIER_FACTOR = 1.25
MAX_TICKER_SPREAD_PCT = 2.0
MIN_VOLUME_24H = LOW_LIQUIDITY_VOLUME_24H
ORDER_BOOK_NOTIONAL_USD = 1_000.0
ORDER_BOOK_MAX_SLIPPAGE_PCT = 1.0
ORDER_BOOK_DEPTH_LIMIT = 50
ORDER_BOOK_CHECK_MAX_ENTRIES = 200


@dataclass
class ScannerQuote:
    base: str
    source: str
    exchange_id: str
    market_type: str
    symbol: str
    buy_price: float
    sell_price: float
    volume_24h: float | None = None
    funding_rate: float | None = None
    timestamp_ms: int = 0
    ticker_spread_pct: float = 0.0


@dataclass
class ScannerEntry:
    base: str
    pos_spread: float | None = None
    pos_buy_source: str = ""
    pos_sell_source: str = ""
    pos_fund_result: float | None = None
    neg_spread: float | None = None
    neg_buy_source: str = ""
    neg_sell_source: str = ""
    neg_fund_result: float | None = None
    max_abs_spread: float = 0.0
    sources_count: int = 0
    volume_24h: float = 0.0
    updated_ms: int = 0
    pos_buy_exchange_id: str = ""
    pos_buy_market_type: str = ""
    pos_buy_symbol: str = ""
    pos_sell_exchange_id: str = ""
    pos_sell_market_type: str = ""
    pos_sell_symbol: str = ""
    neg_buy_exchange_id: str = ""
    neg_buy_market_type: str = ""
    neg_buy_symbol: str = ""
    neg_sell_exchange_id: str = ""
    neg_sell_market_type: str = ""
    neg_sell_symbol: str = ""


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _funding_from_ticker(ticker: dict) -> float | None:
    for key in ("fundingRate", "funding_rate"):
        rate = _safe_float(ticker.get(key))
        if rate is not None:
            return rate * 100
    info = ticker.get("info") or {}
    for key in ("fundingRate", "funding_rate", "lastFundingRate"):
        rate = _safe_float(info.get(key))
        if rate is not None:
            return rate * 100
    return None


def _quote_from_ticker(
    exchange_id: str,
    market_type: str,
    symbol: str,
    market: dict,
    ticker: dict,
    now_ms: int,
) -> ScannerQuote | None:
    base = (market.get("base") or "").upper()
    quote = (market.get("quote") or "").upper()
    settle = (market.get("settle") or "").upper()
    if not base or (quote not in QUOTE_CODES and settle not in QUOTE_CODES):
        return None

    bid = _safe_float(ticker.get("bid"))
    ask = _safe_float(ticker.get("ask"))
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None
    ticker_spread = (ask - bid) / ask * 100
    if ticker_spread > MAX_TICKER_SPREAD_PCT:
        return None

    buy_price = ask
    sell_price = bid
    volume = _safe_float(ticker.get("quoteVolume"))
    if volume is None:
        base_volume = _safe_float(ticker.get("baseVolume"))
        if base_volume is not None:
            volume = base_volume * ((bid + ask) / 2)
    if volume is not None and volume < MIN_VOLUME_24H:
        return None
    return ScannerQuote(
        base=base,
        source=f"{exchange_id}_{market_type}",
        exchange_id=exchange_id,
        market_type=market_type,
        symbol=symbol,
        buy_price=buy_price,
        sell_price=sell_price,
        volume_24h=volume,
        funding_rate=_funding_from_ticker(ticker) if market_type == "perp" else None,
        timestamp_ms=int(ticker.get("timestamp") or now_ms),
        ticker_spread_pct=ticker_spread,
    )


def compute_scanner_entries(quotes: list[ScannerQuote], top_n: int = 100) -> list[ScannerEntry]:
    by_base: dict[str, list[ScannerQuote]] = {}
    for quote in quotes:
        by_base.setdefault(quote.base, []).append(quote)

    entries: list[ScannerEntry] = []
    for base, items in by_base.items():
        items = _filter_price_outliers(items)
        buy_candidates = [q for q in items if q.buy_price > 0]
        sell_candidates = [q for q in items if q.sell_price > 0 and q.market_type == "perp"]
        if not buy_candidates or not sell_candidates:
            continue

        best_pos = None
        best_neg = None
        for buy in buy_candidates:
            for sell in sell_candidates:
                if buy.source == sell.source:
                    continue
                spread = (sell.sell_price - buy.buy_price) / buy.buy_price * 100
                if best_pos is None or spread > best_pos[0]:
                    best_pos = (spread, buy, sell)
                if best_neg is None or spread < best_neg[0]:
                    best_neg = (spread, buy, sell)

        if best_pos is None and best_neg is None:
            continue

        entry = ScannerEntry(
            base=base,
            sources_count=len(items),
            volume_24h=sum(q.volume_24h or 0.0 for q in items),
            updated_ms=max(q.timestamp_ms for q in items),
        )
        if best_pos is not None:
            spread, buy, sell = best_pos
            entry.pos_spread = spread
            entry.pos_buy_source = source_label(buy.exchange_id, buy.market_type)
            entry.pos_sell_source = source_label(sell.exchange_id, sell.market_type)
            entry.pos_fund_result = _fund_result(buy, sell)
            entry.pos_buy_exchange_id = buy.exchange_id
            entry.pos_buy_market_type = buy.market_type
            entry.pos_buy_symbol = buy.symbol
            entry.pos_sell_exchange_id = sell.exchange_id
            entry.pos_sell_market_type = sell.market_type
            entry.pos_sell_symbol = sell.symbol
        if best_neg is not None:
            spread, buy, sell = best_neg
            entry.neg_spread = spread
            entry.neg_buy_source = source_label(buy.exchange_id, buy.market_type)
            entry.neg_sell_source = source_label(sell.exchange_id, sell.market_type)
            entry.neg_fund_result = _fund_result(buy, sell)
            entry.neg_buy_exchange_id = buy.exchange_id
            entry.neg_buy_market_type = buy.market_type
            entry.neg_buy_symbol = buy.symbol
            entry.neg_sell_exchange_id = sell.exchange_id
            entry.neg_sell_market_type = sell.market_type
            entry.neg_sell_symbol = sell.symbol

        entry.max_abs_spread = max(
            abs(entry.pos_spread or 0.0),
            abs(entry.neg_spread or 0.0),
        )
        entries.append(entry)

    entries.sort(key=lambda e: e.max_abs_spread, reverse=True)
    if top_n <= 0:
        return entries
    return entries[:top_n]


def _filter_price_outliers(quotes: list[ScannerQuote]) -> list[ScannerQuote]:
    if len(quotes) < 2:
        return quotes

    mids = [((q.buy_price + q.sell_price) / 2) for q in quotes if q.buy_price > 0 and q.sell_price > 0]
    if len(mids) < 2:
        return quotes

    center = median(mids)
    if center <= 0:
        return quotes

    low = center / PRICE_OUTLIER_FACTOR
    high = center * PRICE_OUTLIER_FACTOR
    return [
        q for q in quotes
        if low <= ((q.buy_price + q.sell_price) / 2) <= high
    ]


def _fund_result(buy: ScannerQuote, sell: ScannerQuote) -> float | None:
    if buy.funding_rate is None and sell.funding_rate is None:
        return None
    return -(buy.funding_rate or 0.0) + (sell.funding_rate or 0.0)


async def fetch_scanner_quotes(exchanges: list[str]) -> tuple[list[ScannerQuote], list[str]]:
    sem = asyncio.Semaphore(MAX_EXCHANGE_CONCURRENCY)
    tasks = []
    for exchange_id in exchanges:
        cfg = EXCHANGE_CONFIGS.get(exchange_id, {})
        if cfg.get("spot"):
            tasks.append(_fetch_exchange_quotes_guarded(sem, exchange_id, "spot"))
        if cfg.get("perp"):
            tasks.append(_fetch_exchange_quotes_guarded(sem, exchange_id, "perp"))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    quotes: list[ScannerQuote] = []
    errors: list[str] = []
    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result)[:120])
            continue
        batch, err = result
        quotes.extend(batch)
        if err:
            errors.append(err)
    return quotes, errors


async def _fetch_exchange_quotes_guarded(sem: asyncio.Semaphore, exchange_id: str, market_type: str):
    async with sem:
        return await _fetch_exchange_quotes(exchange_id, market_type)


async def _fetch_exchange_quotes(exchange_id: str, market_type: str) -> tuple[list[ScannerQuote], str | None]:
    import ccxt.async_support as ccxt_a

    exc = getattr(ccxt_a, exchange_id)(
        ccxt_config(exchange_history_options(exchange_id, market_type), timeout=30_000)
    )
    try:
        await async_retry(lambda: exc.load_markets(), f"{exchange_id}_{market_type} load_markets")
        tickers = await async_retry(lambda: exc.fetch_tickers(), f"{exchange_id}_{market_type} fetch_tickers")
        now_ms = int(time.time() * 1000)
        quotes = []
        for symbol, ticker in (tickers or {}).items():
            market = exc.markets.get(symbol)
            if not market or not _market_matches(market, market_type):
                continue
            quote = _quote_from_ticker(exchange_id, market_type, symbol, market, ticker or {}, now_ms)
            if quote:
                quotes.append(quote)
        return quotes, None
    except Exception as exc_err:
        return [], format_network_error(f"{exchange_id}_{market_type}", "fetch_tickers", exc_err)[:240]
    finally:
        await exc.close()


def _market_matches(market: dict, market_type: str) -> bool:
    if not market.get("active", True):
        return False
    if market_type == "spot":
        return bool(market.get("spot"))
    return bool(market.get("swap"))


async def scan_market(exchanges: list[str], top_n: int = 100) -> tuple[list[ScannerEntry], list[str]]:
    quotes, errors = await fetch_scanner_quotes(exchanges)
    candidate_limit = _candidate_limit_for_liquidity(top_n)
    candidates = compute_scanner_entries(quotes, top_n=candidate_limit)
    filtered, liquidity_errors = await filter_entries_by_order_book_liquidity(candidates, top_n=top_n)
    return filtered, errors + liquidity_errors


def _candidate_limit_for_liquidity(top_n: int) -> int:
    if top_n <= 0:
        return ORDER_BOOK_CHECK_MAX_ENTRIES
    return min(max(top_n * 3, top_n), ORDER_BOOK_CHECK_MAX_ENTRIES)


@dataclass
class OrderBookLiquidity:
    ask_slippage_pct: float | None
    bid_slippage_pct: float | None

    @property
    def ask_ok(self) -> bool:
        return self.ask_slippage_pct is not None and self.ask_slippage_pct <= ORDER_BOOK_MAX_SLIPPAGE_PCT

    @property
    def bid_ok(self) -> bool:
        return self.bid_slippage_pct is not None and self.bid_slippage_pct <= ORDER_BOOK_MAX_SLIPPAGE_PCT


def order_book_slippage_pct(levels: list, notional_usd: float, side: str) -> float | None:
    if not levels or notional_usd <= 0:
        return None
    best_price = _safe_float(levels[0][0])
    if best_price is None or best_price <= 0:
        return None

    remaining = notional_usd
    qty_total = 0.0
    quote_total = 0.0
    for level in levels:
        if len(level) < 2:
            continue
        price = _safe_float(level[0])
        amount = _safe_float(level[1])
        if price is None or amount is None or price <= 0 or amount <= 0:
            continue
        level_quote = price * amount
        take_quote = min(remaining, level_quote)
        take_qty = take_quote / price
        qty_total += take_qty
        quote_total += take_quote
        remaining -= take_quote
        if remaining <= 1e-9:
            break

    if remaining > 1e-6 or qty_total <= 0:
        return None
    avg_price = quote_total / qty_total
    if side == "ask":
        return max(0.0, (avg_price - best_price) / best_price * 100)
    return max(0.0, (best_price - avg_price) / best_price * 100)


def order_book_liquidity(order_book: dict) -> OrderBookLiquidity:
    return OrderBookLiquidity(
        ask_slippage_pct=order_book_slippage_pct(order_book.get("asks") or [], ORDER_BOOK_NOTIONAL_USD, "ask"),
        bid_slippage_pct=order_book_slippage_pct(order_book.get("bids") or [], ORDER_BOOK_NOTIONAL_USD, "bid"),
    )


async def filter_entries_by_order_book_liquidity(
    entries: list[ScannerEntry],
    top_n: int = 100,
) -> tuple[list[ScannerEntry], list[str]]:
    if not entries:
        return [], []
    checks, errors = await _fetch_order_book_liquidity_for_entries(entries)
    filtered: list[ScannerEntry] = []
    for entry in entries:
        pos_ok = _entry_route_liquid(entry, "pos", checks)
        neg_ok = _entry_route_liquid(entry, "neg", checks)
        if not pos_ok:
            entry.pos_spread = None
            entry.pos_buy_source = entry.pos_sell_source = ""
            entry.pos_fund_result = None
        if not neg_ok:
            entry.neg_spread = None
            entry.neg_buy_source = entry.neg_sell_source = ""
            entry.neg_fund_result = None
        entry.max_abs_spread = max(abs(entry.pos_spread or 0.0), abs(entry.neg_spread or 0.0))
        if entry.pos_spread is not None or entry.neg_spread is not None:
            filtered.append(entry)

    filtered.sort(key=lambda e: e.max_abs_spread, reverse=True)
    if top_n > 0:
        filtered = filtered[:top_n]
    return filtered, errors[:20]


def _entry_route_liquid(
    entry: ScannerEntry,
    prefix: str,
    checks: dict[tuple[str, str, str], OrderBookLiquidity],
) -> bool:
    spread = getattr(entry, f"{prefix}_spread")
    if spread is None:
        return False
    buy_key = (
        getattr(entry, f"{prefix}_buy_exchange_id"),
        getattr(entry, f"{prefix}_buy_market_type"),
        getattr(entry, f"{prefix}_buy_symbol"),
    )
    sell_key = (
        getattr(entry, f"{prefix}_sell_exchange_id"),
        getattr(entry, f"{prefix}_sell_market_type"),
        getattr(entry, f"{prefix}_sell_symbol"),
    )
    buy_liq = checks.get(buy_key)
    sell_liq = checks.get(sell_key)
    return bool(buy_liq and sell_liq and buy_liq.ask_ok and sell_liq.bid_ok)


async def _fetch_order_book_liquidity_for_entries(
    entries: list[ScannerEntry],
) -> tuple[dict[tuple[str, str, str], OrderBookLiquidity], list[str]]:
    import ccxt.async_support as ccxt_a

    needed: dict[tuple[str, str], set[str]] = {}
    for entry in entries:
        for prefix in ("pos", "neg"):
            for side in ("buy", "sell"):
                exchange_id = getattr(entry, f"{prefix}_{side}_exchange_id")
                market_type = getattr(entry, f"{prefix}_{side}_market_type")
                symbol = getattr(entry, f"{prefix}_{side}_symbol")
                if exchange_id and market_type and symbol:
                    needed.setdefault((exchange_id, market_type), set()).add(symbol)

    checks: dict[tuple[str, str, str], OrderBookLiquidity] = {}
    errors: list[str] = []

    async def fetch_group(exchange_id: str, market_type: str, symbols: set[str]):
        exc = getattr(ccxt_a, exchange_id)(
            ccxt_config(exchange_history_options(exchange_id, market_type), timeout=15_000)
        )
        try:
            await async_retry(lambda: exc.load_markets(), f"{exchange_id}_{market_type} load_markets")
            for symbol in sorted(symbols):
                try:
                    if symbol not in exc.markets:
                        continue
                    order_book = await async_retry(
                        lambda symbol=symbol: exc.fetch_order_book(symbol, limit=ORDER_BOOK_DEPTH_LIMIT),
                        f"{exchange_id}_{market_type} {symbol} fetch_order_book",
                    )
                    checks[(exchange_id, market_type, symbol)] = order_book_liquidity(order_book or {})
                except Exception as exc_err:
                    errors.append(format_network_error(
                        f"{exchange_id}_{market_type}",
                        f"fetch_order_book {symbol}",
                        exc_err,
                    )[:240])
        except Exception as exc_err:
            errors.append(format_network_error(f"{exchange_id}_{market_type}", "load_markets", exc_err)[:240])
        finally:
            await exc.close()

    await asyncio.gather(*(fetch_group(ex, mkt, symbols) for (ex, mkt), symbols in needed.items()))
    return checks, errors
