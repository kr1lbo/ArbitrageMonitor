import os
import sys
import unittest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication, QWidget

from core.exchange import SpreadEntry
from gui.display import DetailMonitorWidget, SpreadPanel


class SpreadPanelGuiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication(sys.argv)

    def test_update_pairs_reapplies_exchange_filters_to_new_rows(self):
        panel = SpreadPanel()
        panel._filter_buy.setText("Binance | Aster")
        panel._filter_sell.setText("Bybit")

        panel.update_pairs([
            SpreadEntry("binance_spot>>bybit_perp", "Binance SPOT", "Bybit PERP", spread_pct=1.0),
            SpreadEntry("aster_perp>>bybit_perp", "Aster PERP", "Bybit PERP", spread_pct=1.5),
            SpreadEntry("gate_spot>>bybit_perp", "Gate SPOT", "Bybit PERP", spread_pct=2.0),
        ])

        hidden_by_key = {}
        for row in range(panel.table.rowCount()):
            item = panel.table.item(row, 0)
            hidden_by_key[item.data(Qt.UserRole)] = panel.table.isRowHidden(row)

        self.assertFalse(hidden_by_key["binance_spot>>bybit_perp"])
        self.assertFalse(hidden_by_key["aster_perp>>bybit_perp"])
        self.assertTrue(hidden_by_key["gate_spot>>bybit_perp"])

    def test_cached_history_window_is_shown_again_after_close(self):
        win = QWidget()
        wins = {"pair": win}

        win.show()
        self.app.processEvents()
        win.close()
        self.app.processEvents()

        self.assertFalse(win.isVisible())
        self.assertTrue(DetailMonitorWidget._activate_cached_window(wins, "pair"))
        self.assertTrue(win.isVisible())
        win.close()


if __name__ == "__main__":
    unittest.main()
