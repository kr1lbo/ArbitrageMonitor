"""
Хранение истории спреда в памяти + загрузка исторических данных с бирж.
При включённом persist закрытые live-бары и загруженная история сохраняются в SQLite.
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Any

from core.config import async_retry, ccxt_config, format_network_error
from core.history_types import OHLCVBar
from core.spread_storage import SpreadHistoryStorage

# ── Структуры данных ──────────────────────────────────────────────────────────

BASE_LIVE_TF_MS = 10_000
TF_SECONDS = {'1s': 1, '10s': 10, '1m': 60, '5m': 300, '15m': 900, '1h': 3600}
TF_MS      = {k: v * 1000 for k, v in TF_SECONDS.items()}
FALLBACK_TFS = ['1m', '5m', '15m', '1h']
FETCH_RETRIES = 3
FETCH_TIMEOUT_MS = 30_000
MAX_OHLCV_BATCH = 1000
MAX_LIVE_BARS_PER_PAIR = 12_000
logger = logging.getLogger(__name__)


# ── Ресемплинг ────────────────────────────────────────────────────────────────

def _resample(bars: list[OHLCVBar], tf_ms: int) -> list[OHLCVBar]:
    """Ресемплинг более мелких баров в заданный таймфрейм."""
    if not bars:
        return []
    fine_bars = [bar for bar in bars if bar.interval_ms <= tf_ms]
    coarse_bars = [bar for bar in bars if bar.interval_ms > tf_ms]

    result: list[OHLCVBar] = []
    bucket = o = h = l = c = None
    for bar in sorted(fine_bars, key=lambda b: b.ts):
        b_ts = (bar.ts // tf_ms) * tf_ms
        if b_ts != bucket:
            if bucket is not None:
                result.append(OHLCVBar(bucket, o, h, l, c, tf_ms))  # type: ignore[arg-type]
            bucket, o, h, l, c = b_ts, bar.open, bar.high, bar.low, bar.close
        else:
            h = max(h, bar.high)   # type: ignore[type-var]
            l = min(l, bar.low)    # type: ignore[type-var]
            c = bar.close
    if bucket is not None:
        result.append(OHLCVBar(bucket, o, h, l, c, tf_ms))  # type: ignore[arg-type]

    if result:
        fine_start = result[0].ts
        fine_end = result[-1].ts
        coarse_bars = [
            bar for bar in coarse_bars
            if bar.ts + bar.interval_ms <= fine_start or bar.ts > fine_end
        ]

    return sorted(coarse_bars + result, key=lambda b: (b.ts, b.interval_ms))


# ── Вычисление спреда из двух OHLCV ──────────────────────────────────────────

def _compute_spread_bars(
    buy_raw: list, sell_raw: list, tf_ms: int
) -> tuple[list[OHLCVBar], list[OHLCVBar]]:
    """
    Выравнивает два OHLCV-списка по временным меткам и вычисляет:
      IN-спред  = покупаем buy, продаём sell  → (sell−buy)/buy×100
      OUT-спред = закрываем позицию           → (buy−sell)/sell×100

    Для OHLC спреда используется «оптимистичная» формула:
      high = (sell_high − buy_low) / buy_low × 100  (лучший возможный спред за бар)
      low  = (sell_low  − buy_high)/ buy_high× 100  (худший возможный спред за бар)
    """
    if not buy_raw or not sell_raw:
        return [], []
    buy_map  = {(r[0] // tf_ms) * tf_ms: r for r in buy_raw}
    sell_map = {(r[0] // tf_ms) * tf_ms: r for r in sell_raw}
    in_bars: list[OHLCVBar] = []
    out_bars: list[OHLCVBar] = []
    for ts in sorted(set(buy_map) & set(sell_map)):
        b = buy_map[ts]
        s = sell_map[ts]
        bo, bh, bl, bc = b[1], b[2], b[3], b[4]
        so, sh, sl, sc = s[1], s[2], s[3], s[4]
        if 0 in (bl, bh, bc, sl, sh, sc):
            continue
        in_bars.append(OHLCVBar(ts,
            (so - bo) / bo * 100,
            (sh - bl) / bl * 100,
            (sl - bh) / bh * 100,
            (sc - bc) / bc * 100,
            tf_ms,
        ))
        out_bars.append(OHLCVBar(ts,
            (bo - so) / so * 100,
            (bh - sl) / sl * 100,
            (bl - sh) / sh * 100,
            (bc - sc) / sc * 100,
            tf_ms,
        ))
    return in_bars, out_bars


# ── Аккумулятор live-данных ───────────────────────────────────────────────────

class SpreadHistoryManager:
    """Хранит историю спредов в памяти в виде OHLCV-баров."""

    def __init__(self, symbol: str = "", persist: bool = False, storage: SpreadHistoryStorage | None = None):
        self._symbol = symbol.upper()
        self._storage = storage if persist else None
        if persist and self._storage is None:
            self._storage = SpreadHistoryStorage()
        self._loaded_pairs: set[str] = set()
        self._hist: dict[str, dict] = {}  # pair_key → historical {'in': [], 'out': []}
        self._live: dict[str, dict] = {}  # pair_key → closed live {'in': [], 'out': []}
        self._cur:  dict[str, dict] = {}  # pair_key → текущий незакрытый бар

    def _load_persisted(self, pair_key: str) -> None:
        if not self._storage or not self._symbol or pair_key in self._loaded_pairs:
            return
        self._loaded_pairs.add(pair_key)
        in_bars = self._storage.load_bars(self._symbol, pair_key, 'in')
        out_bars = self._storage.load_bars(self._symbol, pair_key, 'out')
        if in_bars or out_bars:
            self._hist[pair_key] = {
                'in': self._merge_bars(self._hist.get(pair_key, {'in': [], 'out': []})['in'], in_bars),
                'out': self._merge_bars(self._hist.get(pair_key, {'in': [], 'out': []})['out'], out_bars),
            }

    @staticmethod
    def _merge_bars(old: list[OHLCVBar], new: list[OHLCVBar]) -> list[OHLCVBar]:
        if not new:
            return list(old)
        by_key = {(bar.ts, bar.interval_ms): bar for bar in old}
        for bar in new:
            by_key[(bar.ts, bar.interval_ms)] = bar
        return sorted(by_key.values(), key=lambda b: (b.ts, b.interval_ms))

    def add_live_spread(self, pair_key: str, ts_ms: int,
                        in_spread: float, out_spread: float) -> None:
        """Вызывается при каждом обновлении live-данных."""
        ts_min = (ts_ms // BASE_LIVE_TF_MS) * BASE_LIVE_TF_MS
        cur = self._cur.get(pair_key)

        if cur is None or cur.get('ts') != ts_min:
            # Закрываем предыдущий бар
            if cur and 'io' in cur:
                d = self._live.setdefault(pair_key, {'in': [], 'out': []})
                in_bar = OHLCVBar(cur['ts'], cur['io'], cur['ih'], cur['il'], cur['ic'], BASE_LIVE_TF_MS)
                out_bar = OHLCVBar(cur['ts'], cur['oo'], cur['oh'], cur['ol'], cur['oc'], BASE_LIVE_TF_MS)
                d['in'].append(in_bar)
                d['out'].append(out_bar)
                if self._storage and self._symbol:
                    self._storage.save_bars(self._symbol, pair_key, 'in', [in_bar])
                    self._storage.save_bars(self._symbol, pair_key, 'out', [out_bar])
                if len(d['in']) > MAX_LIVE_BARS_PER_PAIR:
                    del d['in'][:-MAX_LIVE_BARS_PER_PAIR]
                    del d['out'][:-MAX_LIVE_BARS_PER_PAIR]
            cur = {'ts': ts_min}
            self._cur[pair_key] = cur

        for pfx, val in (('i', in_spread), ('o', out_spread)):
            if pfx + 'o' not in cur:
                cur[pfx+'o'] = cur[pfx+'h'] = cur[pfx+'l'] = cur[pfx+'c'] = val
            else:
                cur[pfx+'h'] = max(cur[pfx+'h'], val)
                cur[pfx+'l'] = min(cur[pfx+'l'], val)
                cur[pfx+'c'] = val

    def set_historical(self, pair_key: str,
                       in_bars: list[OHLCVBar], out_bars: list[OHLCVBar]) -> None:
        """Сохраняет исторические данные, смерживая с накопленными live-барами."""
        self._load_persisted(pair_key)
        existing = self._hist.get(pair_key, {'in': [], 'out': []})

        def merge(old: list[OHLCVBar], new: list[OHLCVBar]) -> list[OHLCVBar]:
            if not new:
                return list(old)
            start, end = new[0].ts, new[-1].ts
            intervals = {b.interval_ms for b in new}
            kept = [
                b for b in old
                if not (b.interval_ms in intervals and start <= b.ts <= end)
            ]
            merged = kept + new
            return sorted(merged, key=lambda b: (b.ts, b.interval_ms))

        self._hist[pair_key] = {
            'in': merge(existing['in'], in_bars),
            'out': merge(existing['out'], out_bars),
        }
        if self._storage and self._symbol:
            self._storage.save_bars(self._symbol, pair_key, 'in', in_bars)
            self._storage.save_bars(self._symbol, pair_key, 'out', out_bars)

    def get_bars(self, pair_key: str,
                 tf: str = '1m') -> tuple[list[OHLCVBar], list[OHLCVBar]]:
        """Возвращает (in_bars, out_bars) для заданного таймфрейма."""
        self._load_persisted(pair_key)
        hist = self._hist.get(pair_key, {'in': [], 'out': []})
        live = self._live.get(pair_key, {'in': [], 'out': []})
        in_bars  = list(hist['in']) + list(live['in'])
        out_bars = list(hist['out']) + list(live['out'])
        cur = self._cur.get(pair_key, {})
        if 'io' in cur:
            in_bars.append( OHLCVBar(cur['ts'], cur['io'], cur['ih'], cur['il'], cur['ic'], BASE_LIVE_TF_MS))
            out_bars.append(OHLCVBar(cur['ts'], cur['oo'], cur['oh'], cur['ol'], cur['oc'], BASE_LIVE_TF_MS))
        tf_ms = TF_MS.get(tf, 60_000)
        return _resample(in_bars, tf_ms), _resample(out_bars, tf_ms)


# ── Загрузка исторических данных с бирж ──────────────────────────────────────

async def fetch_historical_spread(
    buy_exc_id: str, buy_mkt: str,
    sell_exc_id: str, sell_mkt: str,
    symbol: str,
    tf: str = '1m',
    limit: int = 1440,
) -> tuple[list[OHLCVBar], list[OHLCVBar]]:
    """Загружает OHLCV с двух бирж и возвращает (in_bars, out_bars)."""
    import ccxt.async_support as ccxt_a
    from core.exchange import make_market_symbol_candidates

    def make(exc_id: str, mkt: str):
        candidates = make_market_symbol_candidates(exc_id, mkt, symbol)
        sym = candidates[0]
        exc  = getattr(ccxt_a, exc_id)(
            ccxt_config(exchange_history_options(exc_id, mkt), timeout=FETCH_TIMEOUT_MS)
        )
        return exc, sym, candidates

    buy_exc,  buy_sym,  buy_candidates  = make(buy_exc_id,  buy_mkt)
    sell_exc, sell_sym, sell_candidates = make(sell_exc_id, sell_mkt)
    try:
        await asyncio.gather(
            _load_markets_for_history(buy_exc, buy_exc.id, buy_exc.options.get('defaultType', 'spot')),
            _load_markets_for_history(sell_exc, sell_exc.id, sell_exc.options.get('defaultType', 'spot')),
        )
        buy_sym = _resolve_symbol(buy_exc, buy_sym, buy_candidates)
        sell_sym = _resolve_symbol(sell_exc, sell_sym, sell_candidates)

        errors: list[str] = []
        for fetch_tf in _candidate_timeframes(buy_exc, sell_exc, tf):
            try:
                fetch_limit = _fetch_limit_for_timeframe(tf, fetch_tf, limit)
                buy_raw, sell_raw = await asyncio.gather(
                    _fetch_ohlcv_paged(buy_exc, buy_sym, fetch_tf, fetch_limit),
                    _fetch_ohlcv_paged(sell_exc, sell_sym, fetch_tf, fetch_limit),
                )
                if buy_raw and sell_raw:
                    return _compute_spread_bars(buy_raw, sell_raw, TF_MS.get(fetch_tf, 60_000))
                errors.append(f'{fetch_tf}: пустой ответ')
            except Exception as exc:
                errors.append(format_network_error(f'{buy_exc_id}/{sell_exc_id}', f'fetch_ohlcv {fetch_tf}', exc))

        raise RuntimeError('Не удалось загрузить общую историю OHLCV. ' + ' | '.join(errors))
    finally:
        await asyncio.gather(buy_exc.close(), sell_exc.close(), return_exceptions=True)


def exchange_history_options(exc_id: str, mkt: str) -> dict[str, Any]:
    """Options that keep ccxt focused on the market type needed for history."""
    default_type = 'spot' if mkt == 'spot' else 'future' if exc_id == 'kucoinfutures' else 'swap'
    opts: dict[str, Any] = {'defaultType': default_type}

    if exc_id == 'binance':
        opts['fetchMarkets'] = {'types': ['spot' if mkt == 'spot' else 'linear']}
    elif exc_id == 'bybit':
        opts['fetchMarkets'] = {'types': ['spot' if mkt == 'spot' else 'linear']}
    elif exc_id == 'okx':
        opts['fetchMarkets'] = {'types': ['spot' if mkt == 'spot' else 'swap']}
    elif exc_id == 'kucoin':
        opts['fetchMarkets'] = {'types': ['spot'], 'fetchTickersFees': False}
    elif exc_id == 'gate':
        opts['fetchMarkets'] = {'types': ['spot' if mkt == 'spot' else 'swap']}
        if mkt != 'spot':
            opts['swap'] = {'fetchMarkets': {'settlementCurrencies': ['usdt']}}
    elif exc_id == 'bitget':
        opts['fetchMarkets'] = {'types': ['spot' if mkt == 'spot' else 'swap']}
    elif exc_id == 'hyperliquid':
        opts['fetchMarkets'] = {'types': ['spot' if mkt == 'spot' else 'swap']}

    return opts


async def _retry(coro_factory, what: str):
    return await async_retry(coro_factory, what)


async def _load_markets_for_history(exchange, exc_id: str, mkt: str) -> None:
    try:
        await _retry(lambda: exchange.load_markets(), f'{exc_id} {mkt} load_markets')
        return
    except Exception:
        if exc_id != 'mexc':
            raise

    # MEXC load_markets() always fetches spot and swap together. If one endpoint
    # has a transient outage, prime ccxt with the market type we actually need.
    if mkt == 'spot':
        markets = await _retry(lambda: exchange.fetch_spot_markets(), 'mexc spot fetch_markets')
    else:
        markets = await _retry(lambda: exchange.fetch_swap_markets(), 'mexc swap fetch_markets')
    exchange.set_markets(markets)


async def _async_fetch_ohlcv(exchange, symbol: str, preferred_tf: str, limit: int) -> list:
    await _load_markets_for_history(exchange, exchange.id, exchange.options.get('defaultType', 'spot'))
    _ensure_symbol(exchange, symbol)

    avail = exchange.timeframes or {}
    errors: list[str] = []
    for tf in [preferred_tf] + [t for t in FALLBACK_TFS if t != preferred_tf]:
        if avail and tf not in avail:
            continue
        try:
            data = await _fetch_ohlcv_paged(exchange, symbol, tf, limit)
            if data:
                return data
            errors.append(f'{tf}: пустой ответ')
        except Exception as exc:
            errors.append(format_network_error(exchange.id, f'fetch_ohlcv {tf}', exc))

    raise RuntimeError(f'{exchange.id}: не удалось загрузить OHLCV для {symbol}. ' + ' | '.join(errors))


def _resolve_symbol(exchange, symbol: str, candidates: list[str] | None = None) -> str:
    symbols = []
    for candidate in candidates or [symbol]:
        if candidate not in symbols:
            symbols.append(candidate)
    for candidate in symbols:
        if candidate in exchange.markets:
            return candidate
    base_prefix = symbol.split('/')[0] + '/'
    similar = ', '.join([s for s in exchange.markets if s.startswith(base_prefix)][:20])
    suffix = f' Доступные похожие: {similar}' if similar else ''
    tried = ', '.join(symbols)
    raise ValueError(f'{exchange.id}: {tried} не найден в markets.{suffix}')


def _ensure_symbol(exchange, symbol: str) -> None:
    _resolve_symbol(exchange, symbol)


def _supports_timeframe(exchange, tf: str) -> bool:
    if (
        getattr(exchange, 'id', None) == 'binance'
        and tf == '1s'
        and getattr(exchange, 'options', {}).get('defaultType') != 'spot'
    ):
        # ccxt advertises 1s globally, but Binance futures returns "Invalid interval".
        return False
    return not exchange.timeframes or tf in exchange.timeframes


def _candidate_timeframes(buy_exchange, sell_exchange, preferred_tf: str) -> list[str]:
    if preferred_tf == '10s':
        candidates = ['10s', '1s', '1m', '5m', '15m', '1h']
    else:
        candidates = [preferred_tf] + [tf for tf in FALLBACK_TFS if tf != preferred_tf]
    return [
        tf for tf in candidates
        if _supports_timeframe(buy_exchange, tf) and _supports_timeframe(sell_exchange, tf)
    ]


def _fetch_limit_for_timeframe(view_tf: str, fetch_tf: str, limit: int) -> int:
    view_ms = TF_MS.get(view_tf, 60_000)
    fetch_ms = TF_MS.get(fetch_tf, view_ms)
    if fetch_ms < view_ms:
        return max(limit, int(limit * (view_ms / fetch_ms)))
    return limit


async def _fetch_ohlcv_paged(exchange, symbol: str, tf: str, limit: int) -> list:
    if limit <= MAX_OHLCV_BATCH:
        return await _retry(
            lambda: exchange.fetch_ohlcv(symbol, tf, limit=limit),
            f'{exchange.id} {symbol} fetch_ohlcv {tf}',
        ) or []

    tf_ms = TF_MS.get(tf, 60_000)
    since = int(time.time() * 1000) - limit * tf_ms
    rows_by_ts: dict[int, list] = {}
    cursor = since

    while len(rows_by_ts) < limit:
        remaining = limit - len(rows_by_ts)
        batch_limit = min(MAX_OHLCV_BATCH, remaining)
        batch = await _retry(
            lambda cursor=cursor, batch_limit=batch_limit: exchange.fetch_ohlcv(
                symbol, tf, since=cursor, limit=batch_limit
            ),
            f'{exchange.id} {symbol} fetch_ohlcv {tf}',
        ) or []
        if not batch:
            break

        previous_count = len(rows_by_ts)
        for row in batch:
            if row and row[0] is not None:
                rows_by_ts[int(row[0])] = row

        last_ts = int(batch[-1][0])
        if len(rows_by_ts) == previous_count or last_ts < cursor:
            break

        cursor = last_ts + tf_ms
        if cursor >= int(time.time() * 1000):
            break

    rows = [rows_by_ts[ts] for ts in sorted(rows_by_ts)]
    return rows[-limit:]


def _history_market_type(exc_id: str, mkt: str) -> str:
    return 'spot' if mkt == 'spot' else 'future' if exc_id == 'kucoinfutures' else 'swap'


def infer_funding_interval_ms(rows: list[dict]) -> int | None:
    """Returns the most common positive distance between funding timestamps."""
    ts_values = sorted({int(r['ts']) for r in rows if r.get('ts') is not None})
    diffs = [
        b - a
        for a, b in zip(ts_values, ts_values[1:])
        if b > a
    ]
    if not diffs:
        return None

    buckets: dict[int, int] = {}
    for diff in diffs:
        # Funding usually lands on whole hours; bucket small timestamp jitter away.
        bucket = round(diff / 3_600_000) * 3_600_000
        bucket = bucket if bucket > 0 else diff
        buckets[bucket] = buckets.get(bucket, 0) + 1
    return max(buckets.items(), key=lambda item: item[1])[0]


def funding_interval_label(rows: list[dict]) -> str:
    interval_ms = infer_funding_interval_ms(rows)
    if interval_ms is None:
        return '—'
    hours = interval_ms / 3_600_000
    if hours.is_integer():
        return f'{int(hours)}h'
    return f'{hours:.1f}h'


def funding_bucket_ts_ms(ts_ms: int) -> int:
    """Funding timestamps from different exchanges can differ by seconds."""
    hour_ms = 3_600_000
    return round(int(ts_ms) / hour_ms) * hour_ms


def normalize_funding_history_rows(rows: list[dict]) -> list[dict]:
    """Normalizes ccxt funding rows into sorted percent values by funding hour."""
    by_ts: dict[int, float] = {}
    for row in rows or []:
        ts = row.get('timestamp')
        rate = row.get('fundingRate')
        if ts is None or rate is None:
            continue
        by_ts[funding_bucket_ts_ms(int(ts))] = float(rate) * 100

    normalized = [
        {'ts': ts, 'rate': rate}
        for ts, rate in sorted(by_ts.items())
    ]
    interval_ms = infer_funding_interval_ms(normalized)
    for row in normalized:
        row['interval_ms'] = interval_ms
    return normalized


async def fetch_funding_history(
    exc_id: str, mkt: str, symbol: str, limit: int = 200, strict: bool = False
) -> list[dict]:
    """Возвращает [{ts, rate}]. Для spot — пустой список."""
    if mkt == 'spot':
        return []
    import ccxt.async_support as ccxt_a
    from core.exchange import make_market_symbol_candidates
    exc  = getattr(ccxt_a, exc_id)(
        ccxt_config(exchange_history_options(exc_id, mkt), timeout=FETCH_TIMEOUT_MS)
    )
    try:
        candidates = make_market_symbol_candidates(exc_id, 'perp', symbol)
        sym = candidates[0]
        await _load_markets_for_history(exc, exc_id, _history_market_type(exc_id, mkt))
        sym = _resolve_symbol(exc, sym, candidates)
        rows = await _retry(
            lambda: exc.fetch_funding_rate_history(sym, limit=limit),
            f'{exc_id} {sym} fetch_funding_rate_history',
        )
        return normalize_funding_history_rows(rows or [])
    except Exception as exc_info:
        msg = format_network_error(exc_id, f'{symbol} fetch_funding_rate_history', exc_info)
        if strict:
            raise RuntimeError(msg) from exc_info
        logger.warning(msg)
        return []
    finally:
        await exc.close()
