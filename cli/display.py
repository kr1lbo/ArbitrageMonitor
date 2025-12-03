"""
Модуль для отображения данных в CLI через rich
"""
import asyncio

from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table

from core.exchange import ExchangeMonitor


class CLIDisplay:
    """
    Класс для отображения данных в консоли
    """

    def __init__(self):
        self.console = Console()
        self.logged_errors = set()

    def print_header(self):
        self.console.print("\n[bold cyan]" + "="*70 + "[/bold cyan]")
        self.console.print("[bold cyan]💹 МОНИТОРИНГ ЦЕН КРИПТОВАЛЮТ[/bold cyan]")
        self.console.print("[bold cyan]" + "="*70 + "[/bold cyan]\n")

    def input_token(self) -> str:
        """
        Запрашивает токен у пользователя

        Returns:
            Токен в формате BTC/USDT
        """
        token = self.console.input(
            "[bold yellow]📝 Введите токен (например, BTC/USDT): [/bold yellow]"
        ).strip()

        if not token:
            self.console.print("[bold red]❌ Ошибка: токен не может быть пустым[/bold red]")
            return ""

        if '/' not in token:
            if token.upper().endswith('USDT'):
                base = token[:-4].upper()
                token = f"{base}/USDT"
            else:
                self.console.print(
                    "[bold red]❌ Ошибка: неверный формат токена. "
                    "Используйте формат BTC/USDT или BTCUSDT[/bold red]"
                )
                return ""

        return token.upper()

    @staticmethod
    def create_price_table(symbol: str, monitor: ExchangeMonitor) -> Table:
        """
        Создает таблицу с ценами

        Args:
            symbol: Торговая пара
            monitor: Монитор бирж

        Returns:
            Таблица для отображения
        """
        table = Table(
            title=f"[bold cyan]💹 Мониторинг цен {symbol}[/bold cyan]",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
            border_style="cyan",
            title_style="bold cyan"
        )

        table.add_column("🏦 Биржа", style="cyan", justify="center", width=20)
        table.add_column("📊 Статус", justify="center", width=20)
        table.add_column("💰 Цена", justify="center", style="bold yellow", width=20)
        table.add_column("📦 Объем 24ч", justify="center", style="blue", width=20)
        table.add_column("🕐 Обновлено", justify="center", style="dim", width=20)

        all_data = list(monitor.get_all_prices().values())
        sorted_data = sorted(
            all_data,
            key=lambda x: x.volume if x.volume is not None else 0,
            reverse=True
        )

        prices = [data.price for data in sorted_data if data.price is not None]
        min_price = min(prices) if prices else None
        max_price = max(prices) if prices else None

        for data in sorted_data:
            if data.status == 'Онлайн':
                status_text = "[bold green]🟢 Онлайн[/bold green]"
            elif data.status == 'Ошибка':
                status_text = "[bold red]🔴 Ошибка[/bold red]"
            else:
                status_text = f"[yellow]🟡 {data.status}[/yellow]"

            if data.price is not None:
                if data.price == min_price and min_price != max_price:
                    price_text = f"[bold green]${data.price:,.4f} 🔽[/bold green]"
                elif data.price == max_price and min_price != max_price:
                    price_text = f"[bold red]${data.price:,.4f} 🔼[/bold red]"
                else:
                    price_text = f"${data.price:,.4f}"

                volume_text = f"{data.volume:,.2f}" if data.volume is not None else "N/A"
                time_text = data.timestamp.strftime('%H:%M:%S') if data.timestamp else "N/A"
            else:
                price_text = "—"
                volume_text = "—"
                time_text = "—"

                if data.error:
                    error_short = data.error[:40] + "..." if len(data.error) > 40 else data.error
                    price_text = f"[dim red]{error_short}[/dim red]"

            table.add_row(
                data.exchange.upper(),
                status_text,
                price_text,
                volume_text,
                time_text
            )

        return table

    def create_display(self, symbol: str, monitor: ExchangeMonitor):
        """
        Создает полное отображение: таблицу + панель с ошибками

        Args:
            symbol: Торговая пара
            monitor: Монитор бирж

        Returns:
            Table с данными по биржам
        """
        table = self.create_price_table(symbol, monitor)

        return table

    async def run_monitor(self, symbol: str, monitor: ExchangeMonitor):
        """
        Запускает мониторинг с живым обновлением таблицы

        Args:
            symbol: Торговая пара
            monitor: Монитор бирж
        """
        monitor_task = asyncio.create_task(monitor.start(symbol))

        with Live(
            self.create_display(symbol, monitor),
            refresh_per_second=2,
            console=self.console
        ) as live:
            try:
                while not monitor_task.done():
                    await asyncio.sleep(0.5)
                    live.update(self.create_display(symbol, monitor))
            except KeyboardInterrupt:
                monitor.stop()
                raise

    def print_stop_message(self):
        self.console.print("\n\n[bold yellow]⏹️  Мониторинг остановлен пользователем[/bold yellow]")

    def print_error(self, error: str):
        """
        Выводит сообщение об ошибке

        Args:
            error: Текст ошибки
        """
        self.console.print(f"\n[bold red]❌ Ошибка: {error}[/bold red]")
