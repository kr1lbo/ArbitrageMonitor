import unittest
from unittest.mock import AsyncMock, patch

from core.scanner import (
    MIN_VOLUME_24H,
    OrderBookLiquidity,
    ScannerQuote,
    _candidate_limit_for_liquidity,
    _entry_route_liquid,
    _market_matches,
    _quote_from_ticker,
    compute_scanner_entries,
    filter_entries_by_order_book_liquidity,
    order_book_slippage_pct,
)


class ScannerCoreTests(unittest.TestCase):
    def test_compute_scanner_entries_finds_positive_and_negative_routes(self):
        quotes = [
            ScannerQuote("RAVE", "binance_spot", "binance", "spot", "RAVE/USDT", 100, 99, 1000, None, 1),
            ScannerQuote("RAVE", "bybit_perp", "bybit", "perp", "RAVE/USDT:USDT", 105, 104, 2000, 0.01, 2),
            ScannerQuote("RAVE", "gate_perp", "gate", "perp", "RAVE/USDT:USDT", 95, 94, 1500, -0.02, 3),
        ]

        entries = compute_scanner_entries(quotes)

        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.base, "RAVE")
        self.assertAlmostEqual(entry.pos_spread, 9.473684210, places=6)
        self.assertIn("Gate", entry.pos_buy_source)
        self.assertIn("Bybit", entry.pos_sell_source)
        self.assertAlmostEqual(entry.neg_spread, -10.476190476, places=6)
        self.assertIn("Bybit", entry.neg_buy_source)
        self.assertIn("Gate", entry.neg_sell_source)
        self.assertAlmostEqual(entry.pos_fund_result, 0.03)

    def test_compute_scanner_entries_requires_perp_sell_side(self):
        quotes = [
            ScannerQuote("AAA", "binance_spot", "binance", "spot", "AAA/USDT", 10, 11),
            ScannerQuote("AAA", "gate_spot", "gate", "spot", "AAA/USDT", 9, 12),
        ]

        self.assertEqual(compute_scanner_entries(quotes), [])

    def test_compute_scanner_entries_zero_limit_returns_all(self):
        quotes = [
            ScannerQuote(str(i), f"binance_spot_{i}", "binance", "spot", f"{i}/USDT", 10, 9)
            for i in range(3)
        ] + [
            ScannerQuote(str(i), f"bybit_perp_{i}", "bybit", "perp", f"{i}/USDT:USDT", 11, 10)
            for i in range(3)
        ]

        self.assertEqual(len(compute_scanner_entries(quotes, top_n=0)), 3)
        self.assertEqual(len(compute_scanner_entries(quotes, top_n=2)), 2)

    def test_compute_scanner_entries_filters_price_outliers(self):
        quotes = [
            ScannerQuote("EDGE", "bybit_spot", "bybit", "spot", "EDGE/USDT", 1.17, 1.16),
            ScannerQuote("EDGE", "bybit_perp", "bybit", "perp", "EDGE/USDT:USDT", 1.18, 1.17),
            ScannerQuote("EDGE", "gate_perp", "gate", "perp", "EDGE/USDT:USDT", 0.104, 0.103),
        ]

        entries = compute_scanner_entries(quotes)

        self.assertEqual(len(entries), 1)
        self.assertLess(entries[0].max_abs_spread, 2)
        self.assertNotIn("Gate", entries[0].pos_sell_source)

    def test_market_matches_perp_only_accepts_swap_contracts(self):
        self.assertTrue(_market_matches({"active": True, "swap": True, "future": False}, "perp"))
        self.assertFalse(_market_matches({"active": True, "swap": False, "future": True}, "perp"))

    def test_quote_from_ticker_filters_low_volume_tokens(self):
        market = {"base": "LOW", "quote": "USDT", "spot": True, "active": True}
        low = _quote_from_ticker(
            "binance", "spot", "LOW/USDT", market,
            {"bid": 1.0, "ask": 1.01, "quoteVolume": MIN_VOLUME_24H - 1},
            1,
        )
        ok = _quote_from_ticker(
            "binance", "spot", "LOW/USDT", market,
            {"bid": 1.0, "ask": 1.01, "quoteVolume": MIN_VOLUME_24H},
            1,
        )

        self.assertIsNone(low)
        self.assertIsNotNone(ok)

    def test_quote_from_ticker_estimates_quote_volume_from_base_volume(self):
        market = {"base": "VOL", "quote": "USDT", "spot": True, "active": True}
        quote = _quote_from_ticker(
            "binance", "spot", "VOL/USDT", market,
            {"bid": 10.0, "ask": 10.0, "baseVolume": 6_000},
            1,
        )

        self.assertIsNotNone(quote)
        self.assertEqual(quote.volume_24h, 60_000)

    def test_order_book_slippage_requires_fillable_notional(self):
        self.assertIsNone(order_book_slippage_pct([[1.0, 100]], 1000, "ask"))
        self.assertLess(order_book_slippage_pct([[1.0, 500], [1.01, 600]], 1000, "ask"), 1.0)
        self.assertGreater(order_book_slippage_pct([[1.0, 500], [0.97, 600]], 1000, "bid"), 1.0)

    def test_entry_route_liquidity_uses_ask_for_buy_and_bid_for_sell(self):
        entry = compute_scanner_entries([
            ScannerQuote("RAVE", "gate_spot", "gate", "spot", "RAVE/USDT", 100, 99),
            ScannerQuote("RAVE", "bitget_perp", "bitget", "perp", "RAVE/USDT:USDT", 105, 104),
        ])[0]
        checks = {
            ("gate", "spot", "RAVE/USDT"): OrderBookLiquidity(ask_slippage_pct=0.5, bid_slippage_pct=5.0),
            ("bitget", "perp", "RAVE/USDT:USDT"): OrderBookLiquidity(ask_slippage_pct=5.0, bid_slippage_pct=0.5),
        }

        self.assertTrue(_entry_route_liquid(entry, "pos", checks))

        checks[("gate", "spot", "RAVE/USDT")] = OrderBookLiquidity(
            ask_slippage_pct=1.5, bid_slippage_pct=0.5
        )
        self.assertFalse(_entry_route_liquid(entry, "pos", checks))

    def test_filter_entries_by_order_book_liquidity_removes_illiquid_routes(self):
        entries = compute_scanner_entries([
            ScannerQuote("RAVE", "gate_spot", "gate", "spot", "RAVE/USDT", 100, 99),
            ScannerQuote("RAVE", "bitget_perp", "bitget", "perp", "RAVE/USDT:USDT", 105, 104),
        ])
        checks = {
            ("gate", "spot", "RAVE/USDT"): OrderBookLiquidity(ask_slippage_pct=2.0, bid_slippage_pct=0.5),
            ("bitget", "perp", "RAVE/USDT:USDT"): OrderBookLiquidity(ask_slippage_pct=0.5, bid_slippage_pct=0.5),
        }

        with patch(
            "core.scanner._fetch_order_book_liquidity_for_entries",
            new=AsyncMock(return_value=(checks, [])),
        ):
            filtered, errors = asyncio_run(filter_entries_by_order_book_liquidity(entries, top_n=10))

        self.assertEqual(filtered, [])
        self.assertEqual(errors, [])

    def test_candidate_limit_for_liquidity_caps_order_book_work(self):
        self.assertEqual(_candidate_limit_for_liquidity(10), 30)
        self.assertGreaterEqual(_candidate_limit_for_liquidity(1000), 100)


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
