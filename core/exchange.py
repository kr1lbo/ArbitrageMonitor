"""
Модуль для работы с биржами через WebSocket
"""
import asyncio
import ccxt.pro as ccxtpro
from datetime import datetime
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass


@dataclass
class PriceData:
    """Данные о цене токена на бирже"""
    exchange: str
    status: str
    price: Optional[float] = None
    volume: Optional[float] = None
    timestamp: Optional[datetime] = None
    error: Optional[str] = None


class ExchangeMonitor:
    """
    Класс для мониторинга цен на биржах через WebSocket
    """

    def __init__(self, exchanges: List[str]):
        """
        Args:
            exchanges: Список названий бирж для мониторинга
        """
        self.exchanges = exchanges
        self.price_data: Dict[str, PriceData] = {}
        self._callbacks: List[Callable[[str, PriceData], None]] = []
        self._tasks: List[asyncio.Task] = []

        for exchange in exchanges:
            self.price_data[exchange] = PriceData(
                exchange=exchange,
                status='Ожидание...',
            )

    def register_callback(self, callback: Callable[[str, PriceData], None]):
        """
        Регистрирует callback для уведомления об обновлении данных

        Args:
            callback: Функция, которая будет вызвана при обновлении данных
                     (принимает имя биржи и PriceData)
        """
        self._callbacks.append(callback)

    def _notify_callbacks(self, exchange_name: str, data: PriceData):
        """Уведомляет все зарегистрированные callbacks"""
        for callback in self._callbacks:
            try:
                callback(exchange_name, data)
            except Exception:
                pass

    async def _watch_exchange(self, exchange_name: str, symbol: str):
        """
        Отслеживает цену на одной бирже

        Args:
            exchange_name: Название биржи
            symbol: Торговая пара (например, BTC/USDT)
        """
        if exchange_name == "hyperliquid":
            symbol = symbol.upper().replace("USDT", "USDC")

        exchange = None
        try:
            exchange_class = getattr(ccxtpro, exchange_name)
            exchange = exchange_class({
                'enableRateLimit': True,
            })

            await exchange.load_markets()

            data = PriceData(
                exchange=exchange_name,
                status='Подключение...',
            )
            self.price_data[exchange_name] = data
            self._notify_callbacks(exchange_name, data)

            while True:
                ticker = await exchange.watch_ticker(symbol)

                data = PriceData(
                    exchange=exchange_name,
                    status='Онлайн',
                    price=ticker['last'],
                    volume=ticker.get('baseVolume'),
                    timestamp=datetime.now(),
                )
                self.price_data[exchange_name] = data
                self._notify_callbacks(exchange_name, data)

        except Exception as e:
            data = PriceData(
                exchange=exchange_name,
                status='Ошибка',
                timestamp=datetime.now(),
                error=str(e),
            )
            self.price_data[exchange_name] = data
            self._notify_callbacks(exchange_name, data)
        finally:
            if exchange:
                await exchange.close()

    async def start(self, symbol: str):
        """
        Запускает мониторинг всех бирж

        Args:
            symbol: Торговая пара (например, BTC/USDT)
        """
        self._tasks = [
            asyncio.create_task(self._watch_exchange(exchange, symbol))
            for exchange in self.exchanges
        ]

        await asyncio.gather(*self._tasks, return_exceptions=True)

    def stop(self):
        """Останавливает мониторинг всех бирж"""
        for task in self._tasks:
            if not task.done():
                task.cancel()

    def get_price_data(self, exchange: str) -> Optional[PriceData]:
        """
        Получает данные о цене для конкретной биржи

        Args:
            exchange: Название биржи

        Returns:
            PriceData или None если биржа не найдена
        """
        return self.price_data.get(exchange)

    def get_all_prices(self) -> Dict[str, PriceData]:
        """
        Получает данные о ценах для всех бирж

        Returns:
            Словарь {название_биржи: PriceData}
        """
        return self.price_data.copy()