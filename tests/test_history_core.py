import unittest
from unittest.mock import AsyncMock

from core.history import (
    OHLCVBar, SpreadHistoryManager,
    _candidate_timeframes, _compute_spread_bars, _fetch_limit_for_timeframe,
    _fetch_ohlcv_paged, _resample, _resolve_symbol,
    funding_bucket_ts_ms, funding_interval_label, infer_funding_interval_ms,
    normalize_funding_history_rows,
)


class HistoryCoreTests(unittest.TestCase):
    def test_compute_spread_bars_aligns_by_timestamp(self):
        buy = [
            [60_000, 100.0, 110.0, 90.0, 105.0, 1.0],
            [120_000, 200.0, 220.0, 180.0, 210.0, 1.0],
        ]
        sell = [
            [60_000, 102.0, 112.0, 92.0, 108.0, 1.0],
            [180_000, 300.0, 330.0, 270.0, 315.0, 1.0],
        ]

        in_bars, out_bars = _compute_spread_bars(buy, sell, 60_000)

        self.assertEqual(len(in_bars), 1)
        self.assertEqual(len(out_bars), 1)
        self.assertEqual(in_bars[0].ts, 60_000)
        self.assertAlmostEqual(in_bars[0].open, 2.0)
        self.assertAlmostEqual(in_bars[0].close, (108.0 - 105.0) / 105.0 * 100)
        self.assertAlmostEqual(out_bars[0].open, (100.0 - 102.0) / 102.0 * 100)

    def test_resample_preserves_ohlc_shape(self):
        bars = [
            OHLCVBar(0, 1.0, 2.0, 0.5, 1.5),
            OHLCVBar(60_000, 1.5, 3.0, 1.0, 2.5),
            OHLCVBar(300_000, 4.0, 5.0, 3.0, 4.5),
        ]

        resampled = _resample(bars, 300_000)

        self.assertEqual(len(resampled), 2)
        self.assertEqual(resampled[0], OHLCVBar(0, 1.0, 3.0, 0.5, 2.5, 300_000))
        self.assertEqual(resampled[1], OHLCVBar(300_000, 4.0, 5.0, 3.0, 4.5, 300_000))

    def test_10s_view_keeps_coarser_historical_bars(self):
        bars = [
            OHLCVBar(0, 1.0, 2.0, 0.5, 1.5, 60_000),
            OHLCVBar(60_000, 2.0, 3.0, 1.5, 2.5, 60_000),
            OHLCVBar(120_000, 3.0, 3.2, 2.8, 3.1, 10_000),
            OHLCVBar(130_000, 3.1, 3.4, 3.0, 3.3, 10_000),
        ]

        resampled = _resample(bars, 10_000)

        self.assertEqual([bar.ts for bar in resampled], [0, 60_000, 120_000, 130_000])
        self.assertEqual(resampled[0].interval_ms, 60_000)
        self.assertEqual(resampled[-1].interval_ms, 10_000)

    def test_live_history_keeps_10_second_bars(self):
        history = SpreadHistoryManager()
        history.add_live_spread("a>>b", 120_100, 1.0, -1.0)
        history.add_live_spread("a>>b", 125_100, 2.0, -2.0)
        history.add_live_spread("a>>b", 130_100, 3.0, -3.0)

        in_bars, out_bars = history.get_bars("a>>b", "10s")

        self.assertEqual([bar.ts for bar in in_bars], [120_000, 130_000])
        self.assertEqual(in_bars[0].open, 1.0)
        self.assertEqual(in_bars[0].high, 2.0)
        self.assertEqual(in_bars[0].close, 2.0)
        self.assertEqual(out_bars[0].low, -2.0)

    def test_10s_history_can_use_1s_fetch_and_resample(self):
        class FakeExchange:
            timeframes = {"1s": "1s", "1m": "1m"}

        self.assertEqual(_candidate_timeframes(FakeExchange(), FakeExchange(), "10s")[:2], ["1s", "1m"])
        self.assertEqual(_fetch_limit_for_timeframe("10s", "1s", 1440), 14_400)

    def test_live_history_merges_with_historical_bars(self):
        history = SpreadHistoryManager()
        history.add_live_spread("a>>b", 120_100, 1.0, -1.0)
        history.add_live_spread("a>>b", 180_100, 2.0, -2.0)

        history.set_historical(
            "a>>b",
            [OHLCVBar(60_000, 0.1, 0.2, 0.0, 0.15)],
            [OHLCVBar(60_000, -0.1, 0.0, -0.2, -0.15)],
        )

        in_bars, out_bars = history.get_bars("a>>b")

        self.assertEqual([bar.ts for bar in in_bars], [60_000, 120_000, 180_000])
        self.assertEqual([bar.ts for bar in out_bars], [60_000, 120_000, 180_000])

    def test_fetch_ohlcv_paged_deduplicates_and_reaches_limit(self):
        import time

        start = (int(time.time() * 1000) // 60_000 - 1440) * 60_000

        class FakeExchange:
            id = "fake"

            def __init__(self):
                self.fetch_ohlcv = AsyncMock(side_effect=[
                    [[start + i * 60_000, 1, 1, 1, 1, 1] for i in range(1000)],
                    [[start + i * 60_000, 1, 1, 1, 1, 1] for i in range(999, 1441)],
                ])

        rows = asyncio_run(_fetch_ohlcv_paged(FakeExchange(), "BTC/USDT", "1m", 1440))

        self.assertEqual(len(rows), 1440)
        self.assertEqual(rows[0][0], start + 60_000)
        self.assertEqual(rows[-1][0], start + 1440 * 60_000)

    def test_resolve_symbol_uses_exchange_specific_fallback_candidates(self):
        class FakeExchange:
            id = "aster"
            markets = {"RAVE/USD1": object()}

        self.assertEqual(
            _resolve_symbol(FakeExchange(), "RAVE/USDT", ["RAVE/USDT", "RAVE/USD1"]),
            "RAVE/USD1",
        )

    def test_normalize_funding_rows_sorts_dedupes_and_converts_to_percent(self):
        rows = normalize_funding_history_rows([
            {"timestamp": 28_800_010, "fundingRate": "0.0002"},
            {"timestamp": 0, "fundingRate": -0.0001},
            {"timestamp": 28_800_900, "fundingRate": 0.0003},
            {"timestamp": None, "fundingRate": 0.5},
        ])

        self.assertEqual([row["ts"] for row in rows], [0, 28_800_000])
        self.assertAlmostEqual(rows[0]["rate"], -0.01)
        self.assertAlmostEqual(rows[1]["rate"], 0.03)
        self.assertEqual(infer_funding_interval_ms(rows), 28_800_000)
        self.assertEqual(funding_interval_label(rows), "8h")

    def test_funding_bucket_rounds_exchange_jitter_to_same_hour(self):
        base = 19 * 3_600_000
        self.assertEqual(funding_bucket_ts_ms(base + 500), base)
        self.assertEqual(funding_bucket_ts_ms(base + 59_000), base)
        self.assertEqual(funding_bucket_ts_ms(base - 59_000), base)


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
