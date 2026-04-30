import unittest

from core.scanner import ScannerQuote, compute_scanner_entries, _market_matches


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


if __name__ == "__main__":
    unittest.main()
