import os
import sys
import unittest
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication, QTabBar, QWidget

from core.exchange import SpreadEntry
from gui import display as display_mod
from gui.display import DetailMonitorWidget, SS, ScannerPanel, SettingsDialog, SpreadPanel


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
        self.assertEqual(
            panel.visible_pair_keys(),
            {"binance_spot>>bybit_perp", "aster_perp>>bybit_perp"},
        )

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

    def test_scanner_reloads_config_before_scan(self):
        calls = []
        panel = ScannerPanel(
            get_exchanges=lambda: [],
            get_top_n=lambda: 100,
            before_scan=lambda: calls.append("reload"),
        )

        panel._scan_once()

        self.assertEqual(calls, ["reload"])
        self.assertEqual(panel._status.text(), "Нет включённых бирж")

    def test_detail_reloads_config_before_start(self):
        calls = []
        widget = DetailMonitorWidget(
            get_enabled=lambda: [],
            get_top_n=lambda: 50,
            get_alert_spread=lambda: 0.0,
            get_sound_path=lambda: "",
            audio=object(),
            before_start=lambda: calls.append("reload"),
        )

        widget.start_current()

        self.assertEqual(calls, ["reload"])
        self.assertIn("Введите токен", widget._status.text())

    def test_settings_dialog_shows_config_path_and_error(self):
        dialog = SettingsDialog(
            exchanges=[],
            enabled=[],
            main_top_n=100,
            detail_top_n=50,
            alert_spread=0.0,
            sound_path="",
            proxy="",
            websocket_proxy="direct",
            config_path="C:/app/config.json",
            config_error="Config load error: bad json",
        )

        self.assertEqual(dialog._config_path.text(), "C:/app/config.json")
        self.assertFalse(dialog._config_error.isHidden())
        self.assertIn("bad json", dialog._config_error.text())

    def _make_main_window(self):
        cfg = {
            "main_top_n": 100,
            "detail_top_n": 50,
            "alert_spread": 0.0,
            "sound_path": "",
            "proxy": "",
            "websocket_proxy": "direct",
        }

        class DummyAudio:
            def set_file(self, _path):
                pass

            def play(self):
                pass

        patches = [
            patch.object(display_mod, "AudioAlert", DummyAudio),
            patch.object(display_mod, "ensure_config", return_value=dict(cfg)),
            patch.object(display_mod, "load_config", return_value=dict(cfg)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        win = display_mod.MainWindow()
        self.addCleanup(win.close)
        return win

    def test_plus_button_adds_blank_detail_tab(self):
        win = self._make_main_window()
        initial_count = win._tabs.count()

        win._open_blank_detail_tab()

        self.assertEqual(win._tabs.count(), initial_count + 1)
        tab = win._tabs.currentWidget()
        self.assertIn(tab, win._detail_tabs)
        self.assertEqual(win._tabs.tabText(win._tabs.indexOf(tab)), "ДЕТАЛЬНО")

    def test_plus_button_is_visually_labeled(self):
        win = self._make_main_window()

        self.assertEqual(win._btn_add_detail.text(), "+ Новая вкладка")
        self.assertEqual(win._btn_add_detail.objectName(), "new_tab_btn")
        self.assertGreaterEqual(win._btn_add_detail.minimumWidth(), 120)

    def test_tabs_have_dark_custom_styles(self):
        self.assertIn("QTabWidget::pane", SS)
        self.assertIn("border: none;", SS)
        self.assertIn("QTabBar::tab:selected", SS)
        self.assertIn("QPushButton#tab_close_btn", SS)
        self.assertIn("background-color: #ff7a86", SS)

    def test_detail_tab_has_visible_red_close_button(self):
        win = self._make_main_window()
        tab = win._tabs.widget(1)
        btn = win._tabs.tabBar().tabButton(win._tabs.indexOf(tab), QTabBar.RightSide)

        self.assertIsNotNone(btn)
        self.assertEqual(btn.text(), "×")
        self.assertEqual(btn.objectName(), "tab_close_btn")
        self.assertEqual(btn.width(), btn.height())
        self.assertEqual(btn.width(), 16)

    def test_detail_tab_renames_to_started_token(self):
        win = self._make_main_window()
        tab = win._open_blank_detail_tab()

        win._on_detail_started(tab, "ethusdt")

        self.assertEqual(win._tabs.tabText(win._tabs.indexOf(tab)), "ETH")
        self.assertIs(win._token_tabs["ETH"], tab)

    def test_detail_tabs_are_closable_but_scanner_is_fixed(self):
        win = self._make_main_window()
        initial_count = win._tabs.count()

        win._close_tab(0)
        self.assertEqual(win._tabs.count(), initial_count)

        detail = win._tabs.widget(1)
        win._close_tab(1)

        self.assertEqual(win._tabs.count(), initial_count - 1)
        self.assertNotIn(detail, win._detail_tabs)


if __name__ == "__main__":
    unittest.main()
