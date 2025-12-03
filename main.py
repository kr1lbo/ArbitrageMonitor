import asyncio
import logging


from core.exchange import ExchangeMonitor
from cli.display import CLIDisplay


EXCHANGES = [
    'bybit',
    'binance',
    'okx',
    'kucoin',
    "hyperliquid"
]

logging.basicConfig(
    filename='errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def main_cli():
    display = CLIDisplay()

    display.print_header()

    token = display.input_token()
    if not token:
        return

    display.console.print(f"\n[bold green]✅ Выбран токен: {token}[/bold green]")

    monitor = ExchangeMonitor(EXCHANGES)

    try:
        asyncio.run(display.run_monitor(token, monitor))
    except KeyboardInterrupt:
        display.print_stop_message()
    except Exception as e:
        display.print_error(str(e))


if __name__ == "__main__":
    main_cli()
