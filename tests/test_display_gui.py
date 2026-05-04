import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication, QLabel, QPushButton, QHeaderView, QTabBar, QWidget

from core.exchange import SpreadEntry
from gui import display as display_mod
from gui.display import DetailMonitorWidget, SS, ScannerPanel, SettingsDialog, SpreadPanel, TickerPanel
from gui.history_window import SpreadHistoryWindow


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

    def test_spread_panel_can_hide_low_liquidity_pairs(self):
        panel = SpreadPanel()
        panel.update_pairs([
            SpreadEntry(
                "liquid_spot>>liquid_perp",
                "Liquid SPOT",
                "Liquid PERP",
                spread_pct=1.0,
                buy_volume_24h=100_000,
                sell_volume_24h=120_000,
            ),
            SpreadEntry(
                "thin_spot>>thin_perp",
                "Thin SPOT",
                "Thin PERP",
                spread_pct=2.0,
                buy_volume_24h=10_000,
                sell_volume_24h=120_000,
            ),
        ])

        panel._hide_low_liq.setChecked(True)

        hidden_by_key = {}
        for row in range(panel.table.rowCount()):
            item = panel.table.item(row, 0)
            hidden_by_key[item.data(Qt.UserRole)] = panel.table.isRowHidden(row)

        self.assertFalse(hidden_by_key["liquid_spot>>liquid_perp"])
        self.assertTrue(hidden_by_key["thin_spot>>thin_perp"])

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

    def test_spread_history_fetch_error_reenables_refresh(self):
        dummy = SimpleNamespace(
            _fetch_completed=False,
            _status_lbl=QLabel(),
            _refresh_btn=QPushButton(),
            _render=lambda auto_range=False: dummy._status_lbl.setText("live status"),
            _worker=object(),
        )
        dummy._refresh_btn.setEnabled(False)

        SpreadHistoryWindow._on_fetch_error(dummy, "gate/bitget failed")

        self.assertTrue(dummy._refresh_btn.isEnabled())
        self.assertEqual(dummy._refresh_btn.text(), "↺ Обновить")
        self.assertTrue(dummy._fetch_completed)
        self.assertIn("Ошибка загрузки", dummy._status_lbl.text())
        self.assertIn("gate/bitget failed", dummy._status_lbl.toolTip())

    def test_spread_history_fetch_finished_reenables_refresh_without_signals(self):
        rendered = []
        dummy = SimpleNamespace(
            _fetch_completed=False,
            _status_lbl=QLabel(),
            _refresh_btn=QPushButton(),
            _render=lambda auto_range=False: rendered.append(auto_range),
            _worker=object(),
        )
        dummy._refresh_btn.setEnabled(False)

        SpreadHistoryWindow._on_fetch_finished(dummy)

        self.assertTrue(dummy._refresh_btn.isEnabled())
        self.assertEqual(dummy._refresh_btn.text(), "↺ Обновить")
        self.assertIsNone(dummy._worker)
        self.assertEqual(rendered, [True])
        self.assertIn("без результата", dummy._status_lbl.text())

    def test_spread_history_ignores_stale_worker_signals(self):
        current_worker = object()
        stale_worker = object()
        rendered = []
        dummy = SimpleNamespace(
            _fetch_completed=False,
            _status_lbl=QLabel(),
            _refresh_btn=QPushButton(),
            _render=lambda auto_range=False: rendered.append(auto_range),
            _worker=current_worker,
        )
        dummy._refresh_btn.setEnabled(False)

        SpreadHistoryWindow._on_fetch_error(dummy, "old error", stale_worker)
        SpreadHistoryWindow._on_fetch_finished(dummy, stale_worker)

        self.assertFalse(dummy._refresh_btn.isEnabled())
        self.assertIs(dummy._worker, current_worker)
        self.assertEqual(rendered, [])

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

    def test_scanner_table_supports_horizontal_scrolling_without_elision(self):
        panel = ScannerPanel(get_exchanges=lambda: [], get_top_n=lambda: 100)

        self.assertEqual(panel.table.textElideMode(), Qt.ElideNone)
        self.assertEqual(panel.table.horizontalScrollBarPolicy(), Qt.ScrollBarAsNeeded)
        self.assertEqual(
            panel.table.horizontalHeader().sectionResizeMode(panel.C_POS_ROUTE),
            QHeaderView.Interactive,
        )

    def test_ticker_panel_source_column_keeps_full_names_scrollable(self):
        panel = TickerPanel()

        self.assertEqual(panel.table.textElideMode(), Qt.ElideNone)
        self.assertEqual(panel.table.horizontalScrollBarPolicy(), Qt.ScrollBarAsNeeded)
        self.assertEqual(panel.table.horizontalHeader().sectionResizeMode(0), QHeaderView.Interactive)
        self.assertGreaterEqual(panel.table.columnWidth(0), 140)

    def test_spread_table_supports_horizontal_scrolling_without_elision(self):
        panel = SpreadPanel()

        self.assertEqual(panel.table.textElideMode(), Qt.ElideNone)
        self.assertEqual(panel.table.horizontalScrollBarPolicy(), Qt.ScrollBarAsNeeded)
        self.assertEqual(panel.table.horizontalHeader().sectionResizeMode(0), QHeaderView.Stretch)
        self.assertEqual(panel.table.horizontalHeader().sectionResizeMode(1), QHeaderView.Stretch)
        self.assertLessEqual(panel.table.columnWidth(6), 40)
        self.assertLessEqual(panel.table.columnWidth(7), 40)

    def test_exchange_filter_inputs_are_capped_to_leave_room_for_sources(self):
        panel = SpreadPanel()

        self.assertEqual(panel._filter_buy.maximumWidth(), 660)
        self.assertEqual(panel._filter_sell.maximumWidth(), 660)

    def test_detail_reloads_config_before_start(self):
        calls = []
        widget = DetailMonitorWidget(
            get_enabled=lambda: [],
            get_top_n=lambda: 50,
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

    def test_detail_tab_has_own_alert_threshold_field(self):
        win = self._make_main_window()
        tab = win._tabs.widget(1)

        self.assertEqual(tab._alert_spread.value(), 0.0)
        self.assertEqual(tab._alert_spread.specialValueText(), "Выкл")
        self.assertEqual(tab._alert_spread.width(), 92)
        tab._alert_spread.setValue(1.25)
        self.assertEqual(tab._alert_spread.value(), 1.25)

    def test_alerting_detail_tab_is_highlighted(self):
        win = self._make_main_window()
        tab = win._tabs.widget(1)
        win._on_detail_started(tab, "BTC")

        win._set_detail_alert(tab, True)

        index = win._tabs.indexOf(tab)
        self.assertEqual(win._tabs.tabText(index), "!! BTC")
        self.assertEqual(win._tabs.tabBar().tabTextColor(index), display_mod.QColor(display_mod.C["red"]))

        win._set_detail_alert(tab, False)
        self.assertEqual(win._tabs.tabText(index), "BTC")

    def test_detail_splitter_reserves_room_for_sources_panel(self):
        win = self._make_main_window()
        detail = win._tabs.widget(1)
        splitter = detail._spread_panel.parentWidget().parentWidget()
        left = splitter.widget(0)
        right = splitter.widget(1)

        self.assertGreaterEqual(left.minimumWidth(), 720)
        self.assertGreaterEqual(right.minimumWidth(), 360)
        self.assertEqual(left.sizePolicy().horizontalStretch(), 1)
        self.assertEqual(right.sizePolicy().horizontalStretch(), 0)
        self.assertFalse(splitter.childrenCollapsible())

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
