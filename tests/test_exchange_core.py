import unittest

from core.exchange import ExchangeMonitor, SpreadEntry


class ExchangeCoreTests(unittest.TestCase):
    def test_get_pairs_ordered_keeps_negative_spreads_with_limit(self):
        monitor = ExchangeMonitor([], top_n=10)
        monitor.pair_order = ["a_spot>>b_perp", "c_spot>>d_perp"]
        monitor.pair_map = {
            "a_spot>>b_perp": SpreadEntry(
                pair_key="a_spot>>b_perp",
                buy_source="A SPOT",
                sell_source="B PERP",
                spread_pct=-0.5,
            ),
            "c_spot>>d_perp": SpreadEntry(
                pair_key="c_spot>>d_perp",
                buy_source="C SPOT",
                sell_source="D PERP",
                spread_pct=-0.2,
            ),
        }

        pairs = monitor.get_pairs_ordered()

        self.assertEqual(len(pairs), 2)
        self.assertTrue(all(pair.spread_pct < 0 for pair in pairs))

    def test_get_pairs_ordered_zero_limit_returns_all_pairs(self):
        monitor = ExchangeMonitor([], top_n=0)
        for index in range(20):
            key = f"src{index}_spot>>dst{index}_perp"
            monitor.pair_order.append(key)
            monitor.pair_map[key] = SpreadEntry(
                pair_key=key,
                buy_source=f"SRC{index} SPOT",
                sell_source=f"DST{index} PERP",
                spread_pct=index / 100,
            )

        self.assertEqual(len(monitor.get_pairs_ordered()), 20)

    def test_limited_pairs_respect_limit_and_rank_by_spread(self):
        monitor = ExchangeMonitor([], top_n=2)
        for key, spread in [
            ("a_spot>>a_perp", 1.0),
            ("b_spot>>b_perp", 5.0),
            ("c_spot>>c_perp", 3.0),
        ]:
            monitor.pair_order.append(key)
            monitor.pair_map[key] = SpreadEntry(
                pair_key=key,
                buy_source=key.split(">>")[0],
                sell_source=key.split(">>")[1],
                spread_pct=spread,
            )

        pairs = monitor.get_pairs_ordered()

        self.assertEqual(len(pairs), 2)
        self.assertEqual([pair.spread_pct for pair in pairs], [5.0, 3.0])

    def test_funding_result_above_threshold_has_priority_over_spread(self):
        monitor = ExchangeMonitor([], top_n=2)
        rows = [
            ("high_spread>>perp", 5.0, None, None),
            ("mid_spread>>perp", 3.0, None, None),
            ("funding_edge>>perp", -2.0, -0.25, None),
        ]
        for key, spread, buy_funding, sell_funding in rows:
            monitor.pair_order.append(key)
            monitor.pair_map[key] = SpreadEntry(
                pair_key=key,
                buy_source=key.split(">>")[0],
                sell_source=key.split(">>")[1],
                spread_pct=spread,
                buy_funding=buy_funding,
                sell_funding=sell_funding,
            )

        pairs = monitor.get_pairs_ordered()

        self.assertEqual(len(pairs), 2)
        self.assertEqual(pairs[0].pair_key, "funding_edge>>perp")
        self.assertEqual(pairs[1].pair_key, "high_spread>>perp")

    def test_spread_entry_detects_low_liquidity_by_quote_volume(self):
        liquid = SpreadEntry(
            pair_key="a>>b",
            buy_source="A",
            sell_source="B",
            buy_volume_24h=60_000,
            sell_volume_24h=70_000,
        )
        low = SpreadEntry(
            pair_key="a>>b",
            buy_source="A",
            sell_source="B",
            buy_volume_24h=49_000,
            sell_volume_24h=70_000,
        )

        self.assertFalse(liquid.is_low_liquidity())
        self.assertTrue(low.is_low_liquidity())


if __name__ == "__main__":
    unittest.main()
