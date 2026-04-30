"""
GUI арбитражного монитора v4
- Сортировка по всем колонкам
- Фильтр по бирже покупки и продажи
"""
import sys
import asyncio
import datetime
import json
import os
import time
from typing import List, Optional, Dict

from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QFont, QColor
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QCheckBox, QDialog, QDialogButtonBox, QScrollArea,
    QFrame, QSpinBox, QSplitter, QAbstractItemView, QDoubleSpinBox,
    QFileDialog,
)

from core.exchange import (
    ExchangeMonitor, SpreadEntry, TickerData,
    EXCHANGE_CONFIGS, EXCHANGE_LABELS,
    source_label, FUND_SHOW_THRESHOLD,
)
from core.history import SpreadHistoryManager
from gui.history_window import SpreadHistoryWindow, FundingHistoryWindow
from main import EXCHANGES as DEFAULT_EXCHANGES

os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"

# ══════════════════════════════════════════════════════════════════════════════
#  Конфиг
# ══════════════════════════════════════════════════════════════════════════════

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config.json")

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_config(data: dict):
    try:
        existing = load_config()
        existing.update(data)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Config save error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  Звуковой сигнал
# ══════════════════════════════════════════════════════════════════════════════

class AudioAlert:
    def __init__(self):
        self._ready = False
        self._sound_path: Optional[str] = None
        try:
            import pygame
            pygame.mixer.init()
            self._pygame = pygame
            self._ready = True
        except Exception as e:
            print(f"AudioAlert: pygame недоступен — {e}")

    def set_file(self, path: str):
        self._sound_path = path

    def play(self):
        if not self._ready or not self._sound_path:
            return
        if not os.path.isfile(self._sound_path):
            return
        try:
            self._pygame.mixer.music.load(self._sound_path)
            self._pygame.mixer.music.play()
        except Exception as e:
            print(f"AudioAlert play error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  Цвета и стили
# ══════════════════════════════════════════════════════════════════════════════

C = {
    "bg":       "#0d0f14",
    "bg2":      "#13161e",
    "bg3":      "#1a1e2a",
    "border":   "#252a38",
    "text":     "#d6dae8",
    "muted":    "#5a607a",
    "accent":   "#4f8ef7",
    "green":    "#3de8a0",
    "green_bg": "#0b1f16",
    "red":      "#f75f6e",
    "red_bg":   "#200d10",
    "yellow":   "#f7c94f",
    "header":   "#1e2233",
    "row_alt":  "#111520",
    "select":   "#1e2d4a",
}

SS = f"""
QMainWindow, QWidget {{
    background-color: {C['bg']};
    color: {C['text']};
    font-family: 'JetBrains Mono', 'Consolas', 'Courier New', monospace;
    font-size: 12px;
}}
QPushButton {{
    background-color: {C['bg3']};
    color: {C['text']};
    border: 1px solid {C['border']};
    padding: 6px 14px;
    border-radius: 4px;
}}
QPushButton:hover {{ background-color: {C['border']}; }}
QPushButton#accent {{
    background-color: {C['accent']};
    color: #fff;
    border: none;
    font-weight: bold;
}}
QPushButton#accent:hover {{ background-color: #3a7de0; }}
QPushButton#clear_btn {{
    background-color: transparent;
    color: {C['muted']};
    border: none;
    padding: 2px 6px;
    font-size: 14px;
}}
QPushButton#clear_btn:hover {{ color: {C['text']}; }}
QTableWidget {{
    background-color: {C['bg2']};
    alternate-background-color: {C['row_alt']};
    gridline-color: {C['border']};
    color: {C['text']};
    border: 1px solid {C['border']};
    selection-background-color: {C['select']};
}}
QHeaderView::section {{
    background-color: {C['header']};
    color: {C['muted']};
    padding: 5px 8px;
    border: none;
    border-bottom: 1px solid {C['border']};
    font-size: 11px;
    letter-spacing: 1px;
}}
QHeaderView::section:hover {{
    background-color: {C['bg3']};
    color: {C['text']};
}}
QLineEdit {{
    background-color: {C['bg3']};
    color: #fff;
    padding: 7px 12px;
    border-radius: 4px;
    border: 1px solid {C['border']};
    font-size: 14px;
}}
QLineEdit:focus {{ border: 1px solid {C['accent']}; }}
QLineEdit#filter_edit {{
    font-size: 12px;
    padding: 4px 8px;
    background-color: {C['bg2']};
}}
QScrollBar:vertical {{
    background: {C['bg']};
    width: 6px;
    border: none;
}}
QScrollBar::handle:vertical {{
    background: {C['border']};
    border-radius: 3px;
    min-height: 20px;
}}
QLabel#title {{
    font-size: 17px;
    font-weight: bold;
    color: {C['accent']};
    letter-spacing: 2px;
}}
QLabel#sub {{ font-size: 11px; color: {C['muted']}; }}
QLabel#filter_lbl {{ font-size: 11px; color: {C['muted']}; }}
QLabel#ok  {{ color: {C['green']}; font-size: 11px; }}
QLabel#err {{ color: {C['red']};   font-size: 11px; }}
QFrame#sep {{ background-color: {C['border']}; max-height: 1px; }}
QCheckBox {{ spacing: 6px; }}
QCheckBox::indicator {{
    width: 14px; height: 14px;
    border: 1px solid {C['border']};
    border-radius: 3px;
    background: {C['bg3']};
}}
QCheckBox::indicator:checked {{
    background: {C['accent']};
    border-color: {C['accent']};
}}
QSpinBox, QDoubleSpinBox {{
    background: {C['bg3']};
    color: {C['text']};
    border: 1px solid {C['border']};
    padding: 4px 8px;
    border-radius: 4px;
}}
QDialog {{ background-color: {C['bg2']}; }}
"""

# ══════════════════════════════════════════════════════════════════════════════
#  Рабочий поток
# ══════════════════════════════════════════════════════════════════════════════

class MonitorWorker(QThread):
    updated = pyqtSignal()
    err     = pyqtSignal(str)

    def __init__(self, base: str, exchanges: List[str], top_n: int = 50):
        super().__init__()
        self.base = base
        self.exchanges = exchanges
        self.top_n = top_n
        self.monitor: Optional[ExchangeMonitor] = None

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.monitor = ExchangeMonitor(
            exchanges=self.exchanges,
            on_update=self.updated.emit,
            top_n=self.top_n,
        )
        try:
            loop.run_until_complete(self.monitor.start(self.base))
        except Exception as e:
            self.err.emit(str(e))
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()
            except Exception:
                pass

    def stop(self):
        if self.monitor:
            self.monitor.stop()
        self.quit()
        self.wait(3000)


# ══════════════════════════════════════════════════════════════════════════════
#  Колонки
# ══════════════════════════════════════════════════════════════════════════════

C_BUY    = 0
C_SELL   = 1
C_SPREAD = 2
C_F_BUY  = 3
C_F_SELL = 4
C_FUND_R = 5
C_HS     = 6
C_HF     = 7

SPREAD_HEADERS = [
    "КУПИТЬ  (LONG)",
    "ПРОДАТЬ  (SHORT)",
    "СПРЕД %",
    "FUND LONG",
    "FUND SHORT",
    "FUND RESULT",
    "HS",
    "HF",
]

# Числовые колонки (для правильной сортировки)
NUMERIC_COLS = {C_SPREAD, C_F_BUY, C_F_SELL, C_FUND_R}


def _mk(text: str, align=Qt.AlignCenter) -> QTableWidgetItem:
    it = QTableWidgetItem(str(text))
    it.setTextAlignment(align | Qt.AlignVCenter)
    return it


def _clr(item: QTableWidgetItem, fg: str, bg: Optional[str] = None):
    item.setForeground(QColor(fg))
    if bg:
        item.setBackground(QColor(bg))
    return item


def _parse_pct(text: str) -> float:
    try:
        return float(text.replace("%", "").replace("+", "").strip())
    except ValueError:
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  Таблица спредов
# ══════════════════════════════════════════════════════════════════════════════

class SpreadTableWidget(QTableWidget):
    row_moved  = pyqtSignal(list)
    hs_clicked = pyqtSignal(str)   # pair_key
    hf_clicked = pyqtSignal(str)   # pair_key

    def __init__(self):
        super().__init__(0, 8)
        self.setHorizontalHeaderLabels(SPREAD_HEADERS)
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(True)
        self.setEditTriggers(QTableWidget.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectRows)
        self.setShowGrid(True)
        self.setSortingEnabled(False)

        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDragDropMode(QAbstractItemView.InternalMove)
        self.setDropIndicatorShown(True)

        hdr = self.horizontalHeader()
        hdr.setSectionResizeMode(C_BUY,    QHeaderView.Stretch)
        hdr.setSectionResizeMode(C_SELL,   QHeaderView.Stretch)
        hdr.setSectionResizeMode(C_SPREAD, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(C_F_BUY,  QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(C_F_SELL, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(C_FUND_R, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(C_HS,     QHeaderView.Fixed)
        hdr.setSectionResizeMode(C_HF,     QHeaderView.Fixed)
        self.setColumnWidth(C_HS, 38)
        self.setColumnWidth(C_HF, 38)
        self.verticalHeader().setDefaultSectionSize(26)

        self._sort_col: int = -1
        self._sort_asc: bool = True
        hdr.setSectionsClickable(True)
        hdr.sectionClicked.connect(self._on_header_click)
        hdr.setSortIndicatorShown(True)
        hdr.setSortIndicator(-1, Qt.AscendingOrder)

        self.cellClicked.connect(self._on_cell_click)

    def _on_cell_click(self, row: int, col: int):
        if col not in (C_HS, C_HF):
            return
        buy_it = self.item(row, C_BUY)
        if not buy_it:
            return
        key = buy_it.data(Qt.UserRole)
        if not key:
            return
        if col == C_HS:
            self.hs_clicked.emit(key)
        else:
            self.hf_clicked.emit(key)

    # ── Drag & drop ───────────────────────────────────────────────────────────

    def dropEvent(self, event):
        src_row = self.currentRow()
        if src_row < 0:
            event.ignore()
            return

        dest_row = self.rowAt(event.pos().y())
        if dest_row < 0:
            dest_row = self.rowCount() - 1
        if src_row == dest_row:
            event.ignore()
            return

        def read_row(r):
            cells = []
            for c in range(self.columnCount()):
                it = self.item(r, c)
                if it:
                    bg_brush = it.background()
                    fg_brush = it.foreground()
                    cells.append({
                        "text":  it.text(),
                        "fg":    fg_brush.color().name() if fg_brush.style() != Qt.NoBrush else None,
                        "bg":    bg_brush.color().name() if bg_brush.style() != Qt.NoBrush else None,
                        "role":  it.data(Qt.UserRole),
                        "align": it.textAlignment(),
                    })
                else:
                    cells.append(None)
            return cells

        def write_row(r, cells):
            for c, cell in enumerate(cells):
                if cell is None:
                    continue
                it = self.item(r, c)
                if it is None:
                    it = QTableWidgetItem()
                    self.setItem(r, c, it)
                it.setText(cell["text"])
                if cell["fg"] is not None:
                    it.setForeground(QColor(cell["fg"]))
                else:
                    it.setData(Qt.ForegroundRole, None)
                if cell["bg"] is not None:
                    it.setBackground(QColor(cell["bg"]))
                else:
                    it.setData(Qt.BackgroundRole, None)
                it.setTextAlignment(cell["align"])
                if cell["role"] is not None:
                    it.setData(Qt.UserRole, cell["role"])

        self.setUpdatesEnabled(False)
        if src_row < dest_row:
            moved = read_row(src_row)
            for r in range(src_row, dest_row):
                write_row(r, read_row(r + 1))
            write_row(dest_row, moved)
        else:
            moved = read_row(src_row)
            for r in range(src_row, dest_row, -1):
                write_row(r, read_row(r - 1))
            write_row(dest_row, moved)
        self.setUpdatesEnabled(True)
        self.setCurrentCell(dest_row, 0)
        event.accept()
        self._emit_order()

    # ── Сортировка ────────────────────────────────────────────────────────────

    def _on_header_click(self, col: int):
        if self._sort_col == col:
            self._sort_asc = not self._sort_asc
        else:
            self._sort_col = col
            self._sort_asc = True
        self.horizontalHeader().setSortIndicator(
            col, Qt.AscendingOrder if self._sort_asc else Qt.DescendingOrder
        )
        self._apply_sort()

    def _apply_sort(self):
        n = self.rowCount()
        if n < 2:
            return

        rows_data = []
        for r in range(n):
            buy_it   = self.item(r, C_BUY)
            pair_key = buy_it.data(Qt.UserRole) if buy_it else ""

            col_it = self.item(r, self._sort_col)
            raw    = col_it.text() if col_it else ""
            sort_key = _parse_pct(raw) if self._sort_col in NUMERIC_COLS else raw.lower()

            cells = []
            for c in range(self.columnCount()):
                it = self.item(r, c)
                if it:
                    bg_brush = it.background()
                    fg_brush = it.foreground()
                    cells.append({
                        "text":  it.text(),
                        "fg":    fg_brush.color().name() if fg_brush.style() != Qt.NoBrush else None,
                        "bg":    bg_brush.color().name() if bg_brush.style() != Qt.NoBrush else None,
                        "role":  it.data(Qt.UserRole),
                        "align": it.textAlignment(),
                    })
                else:
                    cells.append(None)
            rows_data.append((sort_key, pair_key, cells))

        rows_data.sort(key=lambda x: x[0], reverse=not self._sort_asc)

        self.setUpdatesEnabled(False)
        for r, (_, pair_key, cells) in enumerate(rows_data):
            for c, cell in enumerate(cells):
                it = self.item(r, c)
                if it is None:
                    it = QTableWidgetItem()
                    self.setItem(r, c, it)
                if cell is not None:
                    it.setText(cell["text"])
                    if cell["fg"] is not None:
                        it.setForeground(QColor(cell["fg"]))
                    else:
                        it.setData(Qt.ForegroundRole, None)
                    if cell["bg"] is not None:
                        it.setBackground(QColor(cell["bg"]))
                    else:
                        it.setData(Qt.BackgroundRole, None)
                    it.setTextAlignment(cell["align"])
                    if c == C_BUY:
                        it.setData(Qt.UserRole, pair_key)
                else:
                    it.setText("")
        self.setUpdatesEnabled(True)
        self._emit_order()

    def _emit_order(self):
        keys = []
        for r in range(self.rowCount()):
            it = self.item(r, C_BUY)
            if it:
                key = it.data(Qt.UserRole)
                if key:
                    keys.append(key)
        self.row_moved.emit(keys)


# ══════════════════════════════════════════════════════════════════════════════
#  Панель спредов (таблица + фильтры)
# ══════════════════════════════════════════════════════════════════════════════

class SpreadPanel(QWidget):
    def __init__(self):
        super().__init__()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(4)

        # ── Строка фильтров ───────────────────────────────────────────────────
        filter_row = QHBoxLayout()
        filter_row.setSpacing(6)

        filter_row.addWidget(self._filter_lbl("Купить:"))
        self._filter_buy = QLineEdit()
        self._filter_buy.setObjectName("filter_edit")
        self._filter_buy.setPlaceholderText("Биржа покупки…")
        self._filter_buy.setClearButtonEnabled(True)
        self._filter_buy.textChanged.connect(self._apply_filter)
        filter_row.addWidget(self._filter_buy, 1)

        filter_row.addSpacing(12)

        filter_row.addWidget(self._filter_lbl("Продать:"))
        self._filter_sell = QLineEdit()
        self._filter_sell.setObjectName("filter_edit")
        self._filter_sell.setPlaceholderText("Биржа продажи…")
        self._filter_sell.setClearButtonEnabled(True)
        self._filter_sell.textChanged.connect(self._apply_filter)
        filter_row.addWidget(self._filter_sell, 1)

        self._clear_btn = QPushButton("✕")
        self._clear_btn.setObjectName("clear_btn")
        self._clear_btn.setToolTip("Сбросить фильтры")
        self._clear_btn.clicked.connect(self._clear_filters)
        filter_row.addWidget(self._clear_btn)

        lay.addLayout(filter_row)

        # ── Таблица ───────────────────────────────────────────────────────────
        self.table = SpreadTableWidget()
        lay.addWidget(self.table)

        self._row_index: Dict[str, int] = {}

    @staticmethod
    def _filter_lbl(text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setObjectName("filter_lbl")
        return lbl

    # ── Фильтрация ────────────────────────────────────────────────────────────

    def _apply_filter(self):
        buy_q  = self._filter_buy.text().strip().lower()
        sell_q = self._filter_sell.text().strip().lower()

        for r in range(self.table.rowCount()):
            buy_it  = self.table.item(r, C_BUY)
            sell_it = self.table.item(r, C_SELL)
            buy_txt  = buy_it.text().lower()  if buy_it  else ""
            sell_txt = sell_it.text().lower() if sell_it else ""

            match = (not buy_q  or buy_q  in buy_txt) and \
                    (not sell_q or sell_q in sell_txt)
            self.table.setRowHidden(r, not match)

    def _clear_filters(self):
        self._filter_buy.clear()
        self._filter_sell.clear()

    # ── Обновление данных ─────────────────────────────────────────────────────

    def update_pairs(self, pairs: List[SpreadEntry]):
        table = self.table
        for entry in pairs:
            if not entry.should_show():
                continue
            key = entry.pair_key

            if key not in self._row_index:
                row = table.rowCount()
                table.insertRow(row)
                self._row_index[key] = row

                buy_item = _mk(entry.buy_source, Qt.AlignLeft)
                buy_item.setData(Qt.UserRole, key)
                table.setItem(row, C_BUY,    buy_item)
                table.setItem(row, C_SELL,   _mk(entry.sell_source, Qt.AlignLeft))
                table.setItem(row, C_SPREAD, _mk(""))
                table.setItem(row, C_F_BUY,  _mk(""))
                table.setItem(row, C_F_SELL, _mk(""))
                table.setItem(row, C_FUND_R, _mk(""))

                hs = _mk("HS")
                hs.setForeground(QColor(C["accent"]))
                hs.setToolTip("История спреда")
                table.setItem(row, C_HS, hs)

                hf = _mk("HF")
                hf.setForeground(QColor(C["accent"]))
                hf.setToolTip("История фандинга")
                table.setItem(row, C_HF, hf)

            row = self._find_row(key)
            if row < 0:
                continue
            self._set_spread(row, entry)

        self._sync_index()
        # Переприменяем фильтр к новым строкам (только скрываем/показываем)
        # Сортировку НЕ переприменяем — она статична после клика на заголовок

    def _find_row(self, key: str) -> int:
        for r in range(self.table.rowCount()):
            it = self.table.item(r, C_BUY)
            if it and it.data(Qt.UserRole) == key:
                return r
        return -1

    def _sync_index(self):
        self._row_index.clear()
        for r in range(self.table.rowCount()):
            it = self.table.item(r, C_BUY)
            if it:
                key = it.data(Qt.UserRole)
                if key:
                    self._row_index[key] = r

    def _set_spread(self, row: int, e: SpreadEntry):
        t = self.table

        def cell(col: int) -> QTableWidgetItem:
            it = t.item(row, col)
            if it is None:
                it = _mk("")
                t.setItem(row, col, it)
            return it

        # СПРЕД
        sp_it = cell(C_SPREAD)
        sp_it.setText(f"{e.spread_pct:+.4f}%")
        if e.spread_pct >= 1.0:
            _clr(sp_it, "#ffffff", C["green_bg"])
        elif e.spread_pct >= 0.3:
            _clr(sp_it, C["yellow"], None)
        elif e.spread_pct > 0:
            _clr(sp_it, C["text"], None)
        else:
            _clr(sp_it, C["muted"], None)

        # FUND LONG
        fb = e.buy_funding
        fb_it = cell(C_F_BUY)
        if fb is not None:
            fb_it.setText(f"{fb:+.4f}%")
            _clr(fb_it, C["green"] if fb < 0 else C["red"], None)
        else:
            fb_it.setText("—")
            _clr(fb_it, C["muted"], None)

        # FUND SHORT
        fs = e.sell_funding
        fs_it = cell(C_F_SELL)
        if fs is not None:
            fs_it.setText(f"{fs:+.4f}%")
            _clr(fs_it, C["green"] if fs > 0 else C["red"], None)
        else:
            fs_it.setText("—")
            _clr(fs_it, C["muted"], None)

        # FUND RESULT
        fr = e.fund_result
        fr_it = cell(C_FUND_R)
        if fr is not None:
            fr_it.setText(f"{fr:+.4f}%")
            if fr > 0:
                _clr(fr_it, C["green"], C["green_bg"])
            elif fr < 0:
                _clr(fr_it, C["red"], C["red_bg"])
            else:
                _clr(fr_it, C["muted"], None)
        else:
            fr_it.setText("—")
            _clr(fr_it, C["muted"], None)

    def current_key_order(self) -> List[str]:
        order = []
        for r in range(self.table.rowCount()):
            it = self.table.item(r, C_BUY)
            if it:
                key = it.data(Qt.UserRole)
                if key:
                    order.append(key)
        return order

    def clear_all(self):
        self.table.setRowCount(0)
        self._row_index.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  Панель статусов источников
# ══════════════════════════════════════════════════════════════════════════════

class TickerPanel(QWidget):
    def __init__(self):
        super().__init__()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(4)

        lbl = QLabel("ИСТОЧНИКИ")
        lbl.setObjectName("sub")
        lbl.setContentsMargins(2, 4, 0, 2)
        lay.addWidget(lbl)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["Источник", "Цена", "Объём 24ч", "Статус"])
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)

        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.Stretch)
        for c in (1, 2, 3):
            hdr.setSectionResizeMode(c, QHeaderView.ResizeToContents)
        self.table.verticalHeader().setDefaultSectionSize(24)
        lay.addWidget(self.table)

        self._rows: Dict[str, int] = {}

    def update_tickers(self, tickers: Dict[str, TickerData]):
        for source, td in tickers.items():
            if source not in self._rows:
                row = self.table.rowCount()
                self.table.insertRow(row)
                self._rows[source] = row
                self.table.setItem(row, 0, _mk(source_label(td.exchange_id, td.market_type), Qt.AlignLeft))

            row = self._rows[source]

            def tcell(col: int) -> QTableWidgetItem:
                it = self.table.item(row, col)
                if it is None:
                    it = _mk("")
                    self.table.setItem(row, col, it)
                return it

            pi = tcell(1)
            if td.price is not None:
                pi.setText(f"${td.price:,.4f}")
                _clr(pi, C["text"])
            else:
                pi.setText("—"); _clr(pi, C["muted"])

            vi = tcell(2)
            if td.volume_24h is not None:
                vi.setText(f"{td.volume_24h:,.0f}"); _clr(vi, C["muted"])
            else:
                vi.setText("—"); _clr(vi, C["muted"])

            si = tcell(3)
            si.setText(td.status)
            _clr(si, {
                "Онлайн": C["green"], "Ошибка": C["red"],
                "Нет пары": C["muted"], "Подключение…": C["yellow"],
            }.get(td.status, C["muted"]))

    def clear_all(self):
        self.table.setRowCount(0)
        self._rows.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  Диалог настроек
# ══════════════════════════════════════════════════════════════════════════════

class SettingsDialog(QDialog):
    def __init__(self, exchanges, enabled, top_n, alert_spread, sound_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки")
        self.setMinimumWidth(380)
        lay = QVBoxLayout(self)
        lay.setSpacing(8)

        lbl = QLabel("Биржи"); lbl.setObjectName("sub")
        lay.addWidget(lbl)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setMaximumHeight(260)
        inner = QWidget()
        il = QVBoxLayout(inner); il.setSpacing(4)

        self._checks: Dict[str, QCheckBox] = {}
        for ex in exchanges:
            cfg = EXCHANGE_CONFIGS.get(ex, {})
            label = EXCHANGE_LABELS.get(ex, ex)
            types = []
            if cfg.get("spot"): types.append("SPOT")
            if cfg.get("perp"): types.append("PERP")
            cb = QCheckBox(f"{label}  [{', '.join(types)}]")
            cb.setChecked(ex in enabled)
            self._checks[ex] = cb
            il.addWidget(cb)

        il.addStretch()
        scroll.setWidget(inner)
        lay.addWidget(scroll)

        sep = QFrame(); sep.setObjectName("sep"); lay.addWidget(sep)

        row = QHBoxLayout()
        row.addWidget(QLabel("Макс. строк:"))
        self._top_n = QSpinBox()
        self._top_n.setRange(5, 200)
        self._top_n.setValue(top_n)
        row.addWidget(self._top_n); row.addStretch()
        lay.addLayout(row)

        sep2 = QFrame(); sep2.setObjectName("sep"); lay.addWidget(sep2)

        alert_lbl = QLabel("Звуковой сигнал"); alert_lbl.setObjectName("sub")
        lay.addWidget(alert_lbl)

        spread_row = QHBoxLayout()
        spread_row.addWidget(QLabel("Сигнальный спред:"))
        self._alert_spread = QDoubleSpinBox()
        self._alert_spread.setRange(0.0, 100.0)
        self._alert_spread.setDecimals(2)
        self._alert_spread.setSingleStep(0.1)
        self._alert_spread.setSuffix(" %")
        self._alert_spread.setValue(alert_spread)
        spread_row.addWidget(self._alert_spread); spread_row.addStretch()
        lay.addLayout(spread_row)

        file_row = QHBoxLayout()
        file_row.addWidget(QLabel("Звуковой файл:"))
        self._sound_edit = QLineEdit()
        self._sound_edit.setPlaceholderText("Выберите mp3/wav файл…")
        self._sound_edit.setText(sound_path or "")
        self._sound_edit.setReadOnly(True)
        file_row.addWidget(self._sound_edit, 1)
        browse_btn = QPushButton("…")
        browse_btn.setFixedWidth(32)
        browse_btn.clicked.connect(self._browse_sound)
        file_row.addWidget(browse_btn)
        lay.addLayout(file_row)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def _browse_sound(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Выберите звуковой файл", "",
            "Аудио файлы (*.mp3 *.wav *.ogg);;Все файлы (*)"
        )
        if path:
            self._sound_edit.setText(path)

    def get_enabled(self):      return [k for k, v in self._checks.items() if v.isChecked()]
    def get_top_n(self):        return self._top_n.value()
    def get_alert_spread(self): return self._alert_spread.value()
    def get_sound_path(self):   return self._sound_edit.text().strip()


# ══════════════════════════════════════════════════════════════════════════════
#  Главное окно
# ══════════════════════════════════════════════════════════════════════════════

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Arbitrage Monitor")
        self.resize(1200, 720)
        self.setMinimumSize(900, 500)

        self._exchanges = list(DEFAULT_EXCHANGES)
        self._enabled   = list(DEFAULT_EXCHANGES)
        self._top_n     = 50
        self._worker: Optional[MonitorWorker] = None
        self._symbol = ""
        self._update_count = 0
        self._history = SpreadHistoryManager()
        self._spread_wins: Dict[str, SpreadHistoryWindow]  = {}
        self._fund_wins:   Dict[str, FundingHistoryWindow] = {}

        cfg = load_config()
        self._alert_spread: float = cfg.get("alert_spread", 1.0)
        self._sound_path:   str   = cfg.get("sound_path", "")
        self._audio = AudioAlert()
        self._audio.set_file(self._sound_path)
        self._alerted: bool = False

        self._render_timer = QTimer()
        self._render_timer.setInterval(33)
        self._render_timer.timeout.connect(self._flush_render)
        self._dirty = False

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(12, 8, 12, 8)
        root.setSpacing(6)

        # Шапка
        hdr = QHBoxLayout()
        t = QLabel("ARBITRAGE MONITOR"); t.setObjectName("title")
        hdr.addWidget(t); hdr.addStretch()
        self._status = QLabel("Не запущен"); self._status.setObjectName("sub")
        hdr.addWidget(self._status)
        root.addLayout(hdr)

        sep = QFrame(); sep.setObjectName("sep"); root.addWidget(sep)

        # Строка ввода токена
        ir = QHBoxLayout(); ir.setSpacing(8)
        self._token = QLineEdit()
        self._token.setPlaceholderText("Токен: BTC / ETH / BTCUSDT …")
        self._token.returnPressed.connect(self._start)
        ir.addWidget(self._token, 1)

        self._btn_start = QPushButton("▶  СТАРТ")
        self._btn_start.setObjectName("accent")
        self._btn_start.clicked.connect(self._start)
        ir.addWidget(self._btn_start)

        self._btn_stop = QPushButton("■  СТОП")
        self._btn_stop.clicked.connect(self._stop)
        self._btn_stop.setEnabled(False)
        ir.addWidget(self._btn_stop)

        self._btn_cfg = QPushButton("⚙  Настройки")
        self._btn_cfg.clicked.connect(self._settings)
        ir.addWidget(self._btn_cfg)
        root.addLayout(ir)

        # Счётчик
        self._upd_lbl = QLabel("")
        self._upd_lbl.setObjectName("sub")
        self._upd_lbl.setAlignment(Qt.AlignRight)
        root.addWidget(self._upd_lbl)

        # Сплиттер
        splitter = QSplitter(Qt.Horizontal)

        left = QWidget()
        ll = QVBoxLayout(left)
        ll.setContentsMargins(0, 0, 4, 0)
        ll.setSpacing(3)

        caption = QLabel("СПРЕДЫ  (перетащите строки чтобы упорядочить)")
        caption.setObjectName("sub")
        ll.addWidget(caption)

        self._spread_panel = SpreadPanel()
        self._spread_panel.table.row_moved.connect(self._on_row_moved)
        self._spread_panel.table.hs_clicked.connect(self._open_spread_history)
        self._spread_panel.table.hf_clicked.connect(self._open_funding_history)
        ll.addWidget(self._spread_panel)
        splitter.addWidget(left)

        right = QWidget()
        rl = QVBoxLayout(right)
        rl.setContentsMargins(4, 0, 0, 0)
        self._ticker_panel = TickerPanel()
        rl.addWidget(self._ticker_panel)
        splitter.addWidget(right)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        root.addWidget(splitter, 1)

        hint = QLabel(
            "FUND LONG: −фандинг = зелёный (получаем)   "
            "FUND SHORT: +фандинг = зелёный (получаем)   "
            "FUND RESULT: чистый P&L за период"
        )
        hint.setObjectName("sub")
        hint.setAlignment(Qt.AlignCenter)
        root.addWidget(hint)

    # ── Управление ─────────────────────────────────────────────────────────────

    def _normalize(self, text: str) -> str:
        t = text.strip().upper()
        if not t: return ""
        if "/" in t: return t.split("/")[0].strip()
        if t.endswith("USDT"): return t[:-4]
        return t

    def _start(self):
        base = self._normalize(self._token.text())
        if not base:
            self._status.setText("⚠ Введите токен")
            return
        self._stop()
        self._symbol = base
        self._update_count = 0
        self._history = SpreadHistoryManager()
        # Закрываем все открытые окна истории
        for win in list(self._spread_wins.values()):
            win.close()
        for win in list(self._fund_wins.values()):
            win.close()
        self._spread_wins.clear()
        self._fund_wins.clear()
        self._spread_panel.clear_all()
        self._ticker_panel.clear_all()
        self._status.setText(f"▶ {base}  •  подключение…")
        self._worker = MonitorWorker(base, self._enabled, self._top_n)
        self._worker.updated.connect(self._mark_dirty)
        self._worker.err.connect(lambda m: self._status.setText(f"❌ {m[:80]}"))
        self._worker.start()
        self._render_timer.start()
        self._btn_start.setEnabled(False)
        self._btn_stop.setEnabled(True)

    def _stop(self):
        self._render_timer.stop()
        if self._worker:
            self._worker.stop()
            self._worker = None
        self._btn_start.setEnabled(True)
        self._btn_stop.setEnabled(False)
        self._status.setText("Остановлен")

    def _mark_dirty(self):
        self._dirty = True

    def _flush_render(self):
        if not self._dirty:
            return
        self._dirty = False
        if not self._worker or not self._worker.monitor:
            return

        self._update_count += 1
        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        self._upd_lbl.setText(f"#{self._update_count}  {now}")

        monitor = self._worker.monitor
        pairs   = monitor.get_pairs_ordered()
        tickers = monitor.get_tickers()

        self._spread_panel.update_pairs(pairs)
        self._ticker_panel.update_tickers(tickers)

        # Накапливаем live-данные для истории
        ts_ms = int(time.time() * 1000)
        for entry in pairs:
            if entry.buy_price > 0 and entry.sell_price > 0:
                out_spread = (entry.buy_price - entry.sell_price) / entry.sell_price * 100
                self._history.add_live_spread(
                    entry.pair_key, ts_ms, entry.spread_pct, out_spread
                )

        online  = sum(1 for td in tickers.values() if td.status == "Онлайн")
        total   = len(tickers)
        visible = sum(
            1 for r in range(self._spread_panel.table.rowCount())
            if not self._spread_panel.table.isRowHidden(r)
        )
        self._status.setText(
            f"▶ {self._symbol}  •  {online}/{total} онлайн  •  {visible} пар"
        )

        if self._alert_spread > 0 and self._sound_path:
            best = max((p.spread_pct for p in pairs), default=0.0)
            if best >= self._alert_spread and not self._alerted:
                self._alerted = True
                self._audio.play()
            elif best < self._alert_spread:
                self._alerted = False

    def _on_row_moved(self, new_order: List[str]):
        if self._worker and self._worker.monitor:
            self._worker.monitor.reorder_pairs(new_order)

    def _settings(self):
        dlg = SettingsDialog(
            self._exchanges, self._enabled, self._top_n,
            self._alert_spread, self._sound_path, self
        )
        dlg.setStyleSheet(SS)
        if dlg.exec_():
            self._enabled      = dlg.get_enabled()
            self._top_n        = dlg.get_top_n()
            self._alert_spread = dlg.get_alert_spread()
            self._sound_path   = dlg.get_sound_path()
            self._audio.set_file(self._sound_path)
            self._alerted = False
            save_config({
                "alert_spread": self._alert_spread,
                "sound_path":   self._sound_path,
            })

    # ── История спреда и фандинга ──────────────────────────────────────────────

    @staticmethod
    def _parse_source(source: str):
        """'binance_spot' → ('binance', 'spot'), 'kucoinfutures_perp' → ('kucoinfutures', 'perp')"""
        parts = source.split('_')
        return '_'.join(parts[:-1]), parts[-1]

    def _open_spread_history(self, pair_key: str):
        if not self._symbol:
            return
        if pair_key in self._spread_wins:
            win = self._spread_wins[pair_key]
            win.raise_()
            win.activateWindow()
            return
        buy_src, sell_src = pair_key.split('>>')
        buy_exc_id,  buy_mkt  = self._parse_source(buy_src)
        sell_exc_id, sell_mkt = self._parse_source(sell_src)
        win = SpreadHistoryWindow(
            pair_key=pair_key,
            buy_label=source_label(buy_exc_id,  buy_mkt),
            sell_label=source_label(sell_exc_id, sell_mkt),
            buy_exc_id=buy_exc_id,   buy_mkt=buy_mkt,
            sell_exc_id=sell_exc_id, sell_mkt=sell_mkt,
            symbol=self._symbol,
            history=self._history,
        )
        win.destroyed.connect(lambda: self._spread_wins.pop(pair_key, None))
        self._spread_wins[pair_key] = win
        win.show()

    def _open_funding_history(self, pair_key: str):
        if not self._symbol:
            return
        if pair_key in self._fund_wins:
            win = self._fund_wins[pair_key]
            win.raise_()
            win.activateWindow()
            return
        buy_src, sell_src = pair_key.split('>>')
        buy_exc_id,  buy_mkt  = self._parse_source(buy_src)
        sell_exc_id, sell_mkt = self._parse_source(sell_src)
        win = FundingHistoryWindow(
            buy_label=source_label(buy_exc_id,  buy_mkt),
            sell_label=source_label(sell_exc_id, sell_mkt),
            buy_exc_id=buy_exc_id,   buy_mkt=buy_mkt,
            sell_exc_id=sell_exc_id, sell_mkt=sell_mkt,
            symbol=self._symbol,
        )
        win.destroyed.connect(lambda: self._fund_wins.pop(pair_key, None))
        self._fund_wins[pair_key] = win
        win.show()

    def closeEvent(self, e):
        self._stop()
        super().closeEvent(e)


# ══════════════════════════════════════════════════════════════════════════════

def run_gui():
    app = QApplication(sys.argv)
    app.setFont(QFont("Consolas", 11))
    app.setStyleSheet(SS)
    w = MainWindow()
    w.show()
    sys.exit(app.exec_())
