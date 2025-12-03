import sys
from typing import List, Dict, Optional

from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QColor
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QCheckBox, QDialog, QDialogButtonBox, QScrollArea,
    QStackedWidget, QLayout
)

from core.exchange import ExchangeMonitor, PriceData
from main import EXCHANGES as DEFAULT_EXCHANGES

import os
os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
os.environ["QT_SCALE_FACTOR"] = "1.25"


DARK_STYLESHEET = """
QMainWindow, QWidget {
    background-color: #151821;
    color: #f3f3f5;
    font-size: 13px;
}
QPushButton {
    background-color: #2a2f3c;
    color: #f3f3f5;
    border: 1px solid #3c4456;
    padding: 5px 9px;
    border-radius: 5px;
}
QPushButton:hover {
    background-color: #343b4a;
}
QTableWidget {
    background-color: #191d27;
    alternate-background-color: #202533;
    gridline-color: #32384a;
    color: #f3f3f5;
    selection-background-color: #343b4a;
    selection-color: #ffffff;
}
QHeaderView::section {
    background-color: #202533;
    color: #f3f3f5;
    padding: 3px 5px;
    border: none;
    border-bottom: 1px solid #32384a;
    font-weight: 500;
}
QLineEdit {
    background-color: #191d27;
    color: #ffffff;
    padding: 6px;
    border-radius: 4px;
    border: 1px solid #3c4456;
}
QLineEdit:focus {
    border: 1px solid #5b79ff;
}
QScrollArea {
    border: none;
}
"""


def normalize_symbol(text: str) -> str:
    """
    Приводит пользовательский ввод к формату BASE/QUOTE.

    Args:
        text: строка вида "BTCUSDT" или "BTC/USDT"

    Returns:
        Стандартизированный символ (например, BTC/USDT)
    """
    text = text.strip().upper()
    if not text:
        return ""
    if "/" in text:
        base, quote = text.split("/", 1)
        return f"{base.strip()}/{quote.strip()}"
    if text.endswith("USDT"):
        base = text[:-4]
        return f"{base}/USDT"
    return text


class ExchangeWorker(QThread):
    """
    Асинхронный воркер для получения обновлений цены от нескольких бирж.
    """

    price_updated = pyqtSignal(str, object)
    error_signal = pyqtSignal(str)

    def __init__(self, symbol: str, exchanges: List[str], parent=None):
        super().__init__(parent)
        self.symbol = symbol
        self.exchanges = exchanges
        self.monitor: Optional[ExchangeMonitor] = None
        self._loop = None

    def run(self):
        """
        Инициализирует asyncio-цикл и запускает мониторинг бирж.
        """
        import asyncio

        async def main():
            self.monitor = ExchangeMonitor(self.exchanges)

            def on_update(exchange_name: str, data: PriceData):
                self.price_updated.emit(exchange_name, data)

            self.monitor.register_callback(on_update)

            try:
                await self.monitor.start(self.symbol)
            except Exception as e:
                self.error_signal.emit(str(e))

        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(main())
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    def stop(self):
        """
        Останавливает мониторинг и завершает рабочий поток.
        """
        if self.monitor and self._loop:
            try:
                self.monitor.stop()
            except Exception:
                pass


class SettingsDialog(QDialog):
    """
    Диалоговое окно настроек выбора бирж и поведения окна.
    """

    def __init__(self, exchanges, enabled, open_in_new_window, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки")

        layout = QVBoxLayout(self)

        self.exchange_checkboxes = {}
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        inner = QWidget()
        inner_layout = QVBoxLayout(inner)

        for ex in exchanges:
            cb = QCheckBox(ex)
            cb.setChecked(ex in enabled)
            self.exchange_checkboxes[ex] = cb
            inner_layout.addWidget(cb)

        inner_layout.addStretch()
        scroll.setWidget(inner)
        layout.addWidget(scroll)

        self.open_new_window = QCheckBox("Открывать токены в новом окне")
        self.open_new_window.setChecked(open_in_new_window)
        layout.addWidget(self.open_new_window)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_enabled_exchanges(self):
        """
        Возвращает список включённых бирж.
        """
        return [k for k, v in self.exchange_checkboxes.items() if v.isChecked()]

    def get_open_new_window(self):
        """
        Возвращает флаг «открывать в новом окне».
        """
        return self.open_new_window.isChecked()


class PriceMonitorWidget(QWidget):
    """
    Таблица мониторинга цен:
    Биржа | Цена | 24ч Объём | Δ % | Обновлено
    """

    def __init__(self):
        super().__init__()
        self.worker: Optional[ExchangeWorker] = None
        self.rows: Dict[str, int] = {}
        self.exchanges_list: List[str] = []
        self.current_prices: Dict[str, float] = {}
        self.baseline_prices: Dict[str, float] = {}
        self.baseline_ready: bool = False
        self.seen_exchanges: set[str] = set()
        self.current_symbol: str = ""

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 4)
        layout.setSpacing(4)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(
            ["Биржа", "Цена", "24ч объём", "Δ %", "Обновлено"]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.resizeSection(0, 60)

        for col in range(1, 5):
            header.setSectionResizeMode(col, QHeaderView.Stretch)

        layout.addWidget(self.table)

        self.footer = QLabel("", alignment=Qt.AlignCenter)
        self.footer.setStyleSheet("color: #a5a9b7; font-size: 11px;")
        layout.addWidget(self.footer)

    def start(self, symbol: str, exchanges: List[str]):
        """
        Запускает мониторинг выбранных бирж для указанной торговой пары.

        Args:
            symbol: торговая пара (например, BTC/USDT)
            exchanges: список задействованных бирж
        """
        self.stop()
        self.current_symbol = symbol
        self.footer.setText(f"{symbol} FUTURES")

        self.table.setRowCount(0)
        self.rows.clear()

        self.exchanges_list = list(exchanges)
        self.current_prices.clear()
        self.baseline_prices.clear()
        self.baseline_ready = False
        self.seen_exchanges.clear()

        self.worker = ExchangeWorker(symbol, exchanges)
        self.worker.price_updated.connect(self.on_update)
        self.worker.start()

    def stop(self):
        """
        Останавливает поток мониторинга.
        """
        if self.worker:
            self.worker.stop()
            self.worker.quit()
            self.worker.wait()
            self.worker = None

    def _get_or_create_item(self, row: int, col: int) -> QTableWidgetItem:
        """
        Возвращает ячейку таблицы или создаёт новую.

        Args:
            row: номер строки
            col: номер столбца

        Returns:
            QTableWidgetItem
        """
        item = self.table.item(row, col)
        if item is None:
            item = QTableWidgetItem()
            self.table.setItem(row, col, item)
        return item

    def on_update(self, exchange: str, data: PriceData):
        """
        Обрабатывает обновление данных от биржи и обновляет таблицу.

        Args:
            exchange: название биржи
            data: структура с ценой, объёмом и временем
        """
        if exchange not in self.rows:
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.rows[exchange] = row
            ex_item = QTableWidgetItem(exchange.capitalize())
            self.table.setItem(row, 0, ex_item)

        row = self.rows[exchange]
        self.seen_exchanges.add(exchange)

        if data.price is not None:
            price_text = f"{data.price:,.4f}".rstrip("0").rstrip(".")
            self.current_prices[exchange] = data.price
        else:
            price_text = "—"
        self._get_or_create_item(row, 1).setText(price_text)

        if data.volume is not None:
            volume_text = f"{data.volume:,.2f}"
        else:
            volume_text = "—"
        self._get_or_create_item(row, 2).setText(volume_text)

        if not self.baseline_ready:
            if self.exchanges_list and len(self.seen_exchanges) == len(self.exchanges_list):
                priced = {ex: p for ex, p in self.current_prices.items() if p is not None}
                if priced:
                    self.baseline_prices = priced
                    self.baseline_ready = True

        delta_item = self._get_or_create_item(row, 3)
        delta_text = "—"

        if self.baseline_ready and data.price is not None:
            base = self.baseline_prices.get(exchange)
            if base and base > 0:
                delta = (data.price - base) / base * 100
                delta_text = f"{delta:+.4f}%"

        delta_item.setText(delta_text)

        if delta_text != "—":
            if delta_text.startswith("+"):
                delta_item.setForeground(QColor("#4de3a1"))
                delta_item.setBackground(QColor("#12281e"))
            elif delta_text.startswith("-"):
                delta_item.setForeground(QColor("#ff6b81"))
                delta_item.setBackground(QColor("#2a1518"))
            else:
                delta_item.setForeground(QColor("#f3f3f5"))
                delta_item.setBackground(QColor("#252a36"))
        else:
            delta_item.setForeground(QColor("#b0b3c1"))
            delta_item.setBackground(QColor("#252a36"))

        time_item = self._get_or_create_item(row, 4)
        if data.timestamp:
            time_item.setText(data.timestamp.strftime("%H:%M:%S"))
        else:
            time_item.setText("—")


class TokenWindow(QMainWindow):
    """
    Окно отображения таблицы мониторинга одной торговой пары.
    """

    def __init__(self, symbol: str, exchanges: List[str]):
        super().__init__()
        self.setWindowTitle(symbol)
        self.setWindowFlag(Qt.WindowStaysOnTopHint, True)

        self.monitor = PriceMonitorWidget()
        self.setCentralWidget(self.monitor.table)

        self.monitor.start(symbol, exchanges)
        self.monitor.table.resizeColumnsToContents()
        self.monitor.table.resizeRowsToContents()
        self.adjustSize()

    def closeEvent(self, e):
        """
        Срабатывает при закрытии окна.
        """
        self.monitor.stop()
        super().closeEvent(e)


class MainWindow(QMainWindow):
    """
    Главное окно приложения с вводом токена и открытием мониторинга.
    """

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Crypto monitor")
        self.resize(420, 120)

        self.exchanges = list(DEFAULT_EXCHANGES)
        self.enabled = list(DEFAULT_EXCHANGES)
        self.open_new_window = True
        self.token_windows: List[TokenWindow] = []

        self.pages = QStackedWidget()
        self.setCentralWidget(self.pages)

        self.input_page = QWidget()
        inp = QVBoxLayout(self.input_page)
        inp.setContentsMargins(12, 12, 12, 12)
        inp.setSpacing(8)

        self.token_edit = QLineEdit()
        self.token_edit.setPlaceholderText("Например: BTC/USDT или BTCUSDT")
        inp.addWidget(self.token_edit)

        btns = QHBoxLayout()
        self.open_btn = QPushButton("Открыть")
        self.settings_btn = QPushButton("⚙ Настройки")
        btns.addWidget(self.open_btn)
        btns.addWidget(self.settings_btn)
        inp.addLayout(btns)

        self.pages.addWidget(self.input_page)

        self.monitor_page = QWidget()
        mlay = QVBoxLayout(self.monitor_page)
        mlay.setContentsMargins(8, 8, 8, 8)
        mlay.setSpacing(4)

        self.back_btn = QPushButton("← Назад")
        self.back_btn.clicked.connect(lambda: self.pages.setCurrentWidget(self.input_page))
        mlay.addWidget(self.back_btn)

        self.monitor_widget = PriceMonitorWidget()
        mlay.addWidget(self.monitor_widget)

        self.pages.addWidget(self.monitor_page)

        self.open_btn.clicked.connect(self.open_token)
        self.settings_btn.clicked.connect(self.show_settings)
        self.token_edit.returnPressed.connect(self.open_token)

    def open_token(self):
        """
        Открывает окно мониторинга выбранного токена.
        """
        token = normalize_symbol(self.token_edit.text())
        if not token:
            return

        if self.open_new_window:
            w = TokenWindow(token, self.enabled)
            w.show()
            self.token_windows.append(w)
            w.destroyed.connect(lambda: self.token_windows.remove(w))
        else:
            self.monitor_widget.start(token, self.enabled)
            self.pages.setCurrentWidget(self.monitor_page)

    def show_settings(self):
        """
        Открывает окно настроек.
        """
        dlg = SettingsDialog(self.exchanges, self.enabled, self.open_new_window, self)
        if dlg.exec_():
            self.enabled = dlg.get_enabled_exchanges()
            self.open_new_window = dlg.get_open_new_window()


def run_gui():
    """
    Запускает графический интерфейс приложения.
    """
    app = QApplication(sys.argv)

    font = QFont()
    font.setPointSize(11)
    app.setFont(font)

    app.setStyleSheet(DARK_STYLESHEET)

    w = MainWindow()
    w.show()
    sys.exit(app.exec_())
