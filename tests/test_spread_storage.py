import os
import tempfile
import unittest

from core.history import OHLCVBar, SpreadHistoryManager
from core.spread_storage import SpreadHistoryStorage


class SpreadStorageTests(unittest.TestCase):
    def test_storage_roundtrips_bars(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = SpreadHistoryStorage(os.path.join(tmp, "history.sqlite3"))
            storage.save_bars(
                "BTC",
                "binance_spot>>bybit_perp",
                "in",
                [OHLCVBar(10_000, 1.0, 2.0, 0.5, 1.5, 10_000)],
            )

            bars = storage.load_bars("BTC", "binance_spot>>bybit_perp", "in")

            self.assertEqual(bars, [OHLCVBar(10_000, 1.0, 2.0, 0.5, 1.5, 10_000)])

    def test_history_manager_persists_closed_live_bars(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = SpreadHistoryStorage(os.path.join(tmp, "history.sqlite3"))
            history = SpreadHistoryManager("BTC", persist=True, storage=storage)
            history.add_live_spread("a>>b", 10_100, 1.0, -1.0)
            history.add_live_spread("a>>b", 20_100, 2.0, -2.0)

            restored = SpreadHistoryManager("BTC", persist=True, storage=storage)
            in_bars, out_bars = restored.get_bars("a>>b", "10s")

            self.assertEqual(len(in_bars), 1)
            self.assertEqual(in_bars[0], OHLCVBar(10_000, 1.0, 1.0, 1.0, 1.0, 10_000))
            self.assertEqual(out_bars[0], OHLCVBar(10_000, -1.0, -1.0, -1.0, -1.0, 10_000))

    def test_history_manager_persists_historical_bars(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = SpreadHistoryStorage(os.path.join(tmp, "history.sqlite3"))
            history = SpreadHistoryManager("ETH", persist=True, storage=storage)
            history.set_historical(
                "x>>y",
                [OHLCVBar(60_000, 0.1, 0.2, 0.0, 0.15, 60_000)],
                [OHLCVBar(60_000, -0.1, 0.0, -0.2, -0.15, 60_000)],
            )

            restored = SpreadHistoryManager("ETH", persist=True, storage=storage)
            in_bars, out_bars = restored.get_bars("x>>y", "1m")

            self.assertEqual(in_bars, [OHLCVBar(60_000, 0.1, 0.2, 0.0, 0.15, 60_000)])
            self.assertEqual(out_bars, [OHLCVBar(60_000, -0.1, 0.0, -0.2, -0.15, 60_000)])


if __name__ == "__main__":
    unittest.main()
