"""
Ядро мониторинга: цены (spot + perp) и фандинг через ccxt
Ключевые улучшения v3:
  - load_markets параллельно (gather) → быстрая инициализация
  - Пары фиксированы (pair_key), значения обновляются на месте
  - Показываем пары с fund_result >= FUND_SHOW_THRESHOLD даже при маленьком спреде
  - Батчинг сигналов: on_update не чаще MAX_UPDATE_HZ раз/сек
"""
import asyncio
import logging
import time
import ccxt.pro as ccxtpro
import ccxt as ccxt_sync
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional, Callable

from core.config import async_retry, ccxt_config, format_network_error, human_error, sync_retry
from core.history import exchange_history_options

logger = logging.getLogger(__name__)

FUND_SHOW_THRESHOLD = 0.20   # показывать пару если fund_result >= этого значения
MIN_SPREAD_PCT      = 0.0    # минимальный спред для показа
MAX_UPDATE_HZ       = 30     # максимальное количество UI-обновлений в секунду

EXCHANGE_CONFIGS: Dict[str, dict] = {
    "binance":        {"spot": True,  "perp": True,  "funding": True},
    "bybit":          {"spot": True,  "perp": True,  "funding": True},
    "okx":            {"spot": True,  "perp": True,  "funding": True},
    "kucoin":         {"spot": True,  "perp": False, "funding": False},  # только спот
    "kucoinfutures":  {"spot": False, "perp": True,  "funding": True},   # только перп/фьюч
    "gate":           {"spot": True,  "perp": True,  "funding": True},
    "mexc":           {"spot": True,  "perp": True,  "funding": True},
    "bitget":         {"spot": True,  "perp": True,  "funding": True},
    "bingx":          {"spot": True,  "perp": True,  "funding": True},
    "hyperliquid":    {"spot": False, "perp": True,  "funding": True},
    "aster":          {"spot": True,  "perp": True,  "funding": True},
    "lighter":        {"spot": True,  "perp": True,  "funding": True},
}

EXCHANGE_LABELS = {
    "binance":        "Binance",
    "bybit":          "Bybit",
    "okx":            "OKX",
    "kucoin":         "KuCoin",
    "kucoinfutures":  "KuCoin Fut",
    "gate":           "Gate",
    "mexc":           "MEXC",
    "bitget":         "Bitget",
    "bingx":          "BingX",
    "hyperliquid":    "HyperLiquid",
    "aster":          "Aster",
    "lighter":        "Lighter",
}


def source_label(exchange_id: str, market_type: str) -> str:
    name = EXCHANGE_LABELS.get(exchange_id, exchange_id.capitalize())
    return f"{name} {'SPOT' if market_type == 'spot' else 'PERP'}"


def make_spot_symbol(exchange_id: str, base: str) -> str:
    if exchange_id in ["lighter", "hyperliquid"]:
        return f"{base}/USDC"
    return f"{base}/USDT"


def make_perp_symbol(exchange_id: str, base: str) -> str:
    if exchange_id in ["hyperliquid", "lighter"]:
        return f"{base}/USDC:USDC"
    # kucoinfutures использует unified ccxt формат RAVE/USDT:USDT
    # (внутренний id биржи RAVEUSDTM — ccxt конвертирует сам)
    return f"{base}/USDT:USDT"


@dataclass
class TickerData:
    source: str
    exchange_id: str
    market_type: str
    symbol: str
    price: Optional[float] = None
    volume_24h: Optional[float] = None
    funding_rate: Optional[float] = None   # уже в %, 0.01 = 0.01%
    timestamp: Optional[datetime] = None
    error: Optional[str] = None
    status: str = "Ожидание"


@dataclass
class SpreadEntry:
    pair_key: str
    buy_source: str
    sell_source: str
    buy_price: float = 0.0
    sell_price: float = 0.0
    spread_pct: float = 0.0
    buy_funding: Optional[float] = None
    sell_funding: Optional[float] = None

    @property
    def fund_result(self) -> Optional[float]:
        """
        Чистый P&L по фандингу за один период.
        Long  (buy side) : получаем если funding < 0, вклад = -buy_funding
        Short (sell side): получаем если funding > 0, вклад = +sell_funding
        Итого > 0 => зарабатываем на фандинге
        """
        if self.buy_funding is None and self.sell_funding is None:
            return None
        b = self.buy_funding or 0.0
        s = self.sell_funding or 0.0
        return round(-b + s, 6)

    def should_show(self) -> bool:
        if self.spread_pct >= MIN_SPREAD_PCT:
            return True
        fr = self.fund_result
        return fr is not None and fr >= FUND_SHOW_THRESHOLD


class ExchangeMonitor:
    def __init__(
        self,
        exchanges: List[str],
        on_update: Optional[Callable] = None,
        top_n: int = 50,
    ):
        self.exchanges = [e for e in exchanges if e in EXCHANGE_CONFIGS]
        self.on_update = on_update
        self.top_n = top_n

        self.tickers: Dict[str, TickerData] = {}
        self.pair_map: Dict[str, SpreadEntry] = {}
        self.pair_order: List[str] = []   # порядок строк (пользователь может менять)

        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._lock = asyncio.Lock()
        self._last_emit = 0.0

    async def start(self, base: str):
        self._running = True
        coros = []
        for ex_id in self.exchanges:
            cfg = EXCHANGE_CONFIGS[ex_id]
            if cfg["spot"]:
                coros.append(self._watch_price(ex_id, "spot", base))
            if cfg["perp"]:
                coros.append(self._watch_price(ex_id, "perp", base))
            if cfg["funding"]:
                coros.append(self._poll_funding(ex_id, base))
        self._tasks = [asyncio.create_task(c) for c in coros]
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()

    async def _watch_price(self, exchange_id: str, market_type: str, base: str):
        source = f"{exchange_id}_{market_type}"
        symbol = (
            make_spot_symbol(exchange_id, base) if market_type == "spot"
            else make_perp_symbol(exchange_id, base)
        )

        async with self._lock:
            self.tickers[source] = TickerData(
                source=source, exchange_id=exchange_id,
                market_type=market_type, symbol=symbol,
                status="Подключение…"
            )

        exchange = None
        retry_delay = 2
        stage = "init"

        while self._running:
            try:
                stage = "create_exchange"
                cls = getattr(ccxtpro, exchange_id)
                if market_type == "spot":
                    default_type = "spot"
                elif exchange_id in ["kucoinfutures", "lighter"]:
                    default_type = "swap" if exchange_id == "lighter" else "future"
                else:
                    default_type = "swap"
                opts = exchange_history_options(exchange_id, market_type)
                opts["defaultType"] = default_type
                exchange = cls(ccxt_config(opts, proxy_kind="websocket"))

                stage = "load_markets"
                await async_retry(lambda: exchange.load_markets(), f"{source} load_markets")

                if symbol not in exchange.markets:
                    async with self._lock:
                        self.tickers[source].status = "Нет пары"
                        self.tickers[source].error = f"{symbol} не найден"
                    self._emit()
                    await exchange.close()
                    return

                async with self._lock:
                    self.tickers[source].status = "Онлайн"
                retry_delay = 2
                self._emit()

                while self._running:
                    stage = "watch_ticker"
                    ticker = await exchange.watch_ticker(symbol)
                    price = ticker.get("last") or ticker.get("close")
                    if price:
                        async with self._lock:
                            td = self.tickers[source]
                            td.price = price
                            td.volume_24h = ticker.get("baseVolume")
                            td.timestamp = datetime.now()
                            td.status = "Онлайн"
                            td.error = None
                        self._recalc_for(source)
                        self._emit()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(format_network_error(source, stage, e, proxy_kind="websocket"))
                async with self._lock:
                    if source in self.tickers:
                        self.tickers[source].status = "Ошибка"
                        self.tickers[source].error = human_error(e)[:120]
                self._emit()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
            finally:
                if exchange:
                    try:
                        await exchange.close()
                    except Exception:
                        pass
                    exchange = None

    async def _poll_funding(self, exchange_id: str, base: str):
        symbol = make_perp_symbol(exchange_id, base)
        source = f"{exchange_id}_perp"

        while self._running:
            try:
                loop = asyncio.get_event_loop()
                rate_pct = await loop.run_in_executor(
                    None, self._fetch_funding_sync, exchange_id, symbol
                )
                if rate_pct is not None:
                    async with self._lock:
                        if source in self.tickers:
                            self.tickers[source].funding_rate = rate_pct
                    self._recalc_all()
                    self._emit()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"funding {exchange_id}: {e}")
            await asyncio.sleep(60)

    @staticmethod
    def _fetch_funding_sync(exchange_id: str, symbol: str) -> Optional[float]:
        ex = None
        try:
            cls = getattr(ccxt_sync, exchange_id)
            default_type = "swap" if exchange_id in ["lighter", "hyperliquid"] else \
                "future" if exchange_id == "kucoinfutures" else "swap"

            ex = cls(ccxt_config({"defaultType": default_type}))
            fr = sync_retry(lambda: ex.fetch_funding_rate(symbol), f"{exchange_id} {symbol} fetch_funding_rate")
            rate = fr.get("fundingRate")
            return rate * 100 if rate is not None else None
        except Exception as e:
            logger.debug(f"funding_sync {exchange_id}: {e}")
            return None
        finally:
            if ex is not None:
                try:
                    ex.close()
                except Exception:
                    pass

    def _emit(self):
        """Вызывает on_update не чаще MAX_UPDATE_HZ раз/сек"""
        now = time.monotonic()
        if now - self._last_emit >= 1.0 / MAX_UPDATE_HZ:
            self._last_emit = now
            if self.on_update:
                self.on_update()

    def _update_pair(self, buy_td: TickerData, sell_td: TickerData):
        if buy_td.price is None or sell_td.price is None:
            return
        # SELL сторона — только perp (шорт на споте невозможен)
        if sell_td.market_type == "spot":
            return
        key = f"{buy_td.source}>>{sell_td.source}"
        spread = (sell_td.price - buy_td.price) / buy_td.price * 100

        if key not in self.pair_map:
            entry = SpreadEntry(
                pair_key=key,
                buy_source=source_label(buy_td.exchange_id, buy_td.market_type),
                sell_source=source_label(sell_td.exchange_id, sell_td.market_type),
                buy_price=buy_td.price,
                sell_price=sell_td.price,
                spread_pct=spread,
                buy_funding=buy_td.funding_rate,
                sell_funding=sell_td.funding_rate,
            )
            self.pair_map[key] = entry
            self.pair_order.append(key)
        else:
            e = self.pair_map[key]
            e.buy_price = buy_td.price
            e.sell_price = sell_td.price
            e.spread_pct = spread
            e.buy_funding = buy_td.funding_rate
            e.sell_funding = sell_td.funding_rate

    def _recalc_for(self, updated_source: str):
        td_u = self.tickers.get(updated_source)
        if not td_u or td_u.price is None:
            return
        for src, td in self.tickers.items():
            if src == updated_source or td.price is None:
                continue
            self._update_pair(td_u, td)
            self._update_pair(td, td_u)

    def _recalc_all(self):
        srcs = list(self.tickers.values())
        for i, a in enumerate(srcs):
            for b in srcs[i+1:]:
                if a.price and b.price:
                    self._update_pair(a, b)
                    self._update_pair(b, a)

    def get_pairs_ordered(self) -> List[SpreadEntry]:
        pairs = [self.pair_map[k] for k in self.pair_order if k in self.pair_map]
        if self.top_n <= 0:
            return pairs
        return pairs[:self.top_n]

    def get_tickers(self) -> Dict[str, TickerData]:
        return dict(self.tickers)

    def reorder_pairs(self, new_order: List[str]):
        self.pair_order = [k for k in new_order if k in self.pair_map]
