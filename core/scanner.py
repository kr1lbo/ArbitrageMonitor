from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from statistics import median
from typing import Any

from core.exchange import EXCHANGE_CONFIGS, source_label
from core.config import async_retry, ccxt_config, format_network_error
from core.history import exchange_history_options


QUOTE_CODES = {"USDT", "USDC"}
MAX_EXCHANGE_CONCURRENCY = 4
PRICE_OUTLIER_FACTOR = 1.25
MAX_TICKER_SPREAD_PCT = 2.0
MIN_VOLUME_24H = 5_000.0


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
    volume = _safe_float(ticker.get("quoteVolume") or ticker.get("baseVolume"))
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
        if best_neg is not None:
            spread, buy, sell = best_neg
            entry.neg_spread = spread
            entry.neg_buy_source = source_label(buy.exchange_id, buy.market_type)
            entry.neg_sell_source = source_label(sell.exchange_id, sell.market_type)
            entry.neg_fund_result = _fund_result(buy, sell)

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
    return compute_scanner_entries(quotes, top_n=top_n), errors
