import sys
import logging
import os


def _runtime_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


logging.basicConfig(
    filename=os.path.join(_runtime_dir(), "errors.log"),
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Все доступные биржи (можно отключать через настройки GUI)
EXCHANGES = [
    "binance",
    "bybit",
    "okx",
    "kucoin",
    "kucoinfutures",
    "gate",
    "mexc",
    "bitget",
    "bingx",
    "hyperliquid",
    "aster",
    "lighter",
]


def main():
    from core.config import ensure_config
    from gui.display import run_gui
    ensure_config()
    run_gui()


if __name__ == "__main__":
    main()
