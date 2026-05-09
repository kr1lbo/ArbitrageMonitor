"""
Окна истории спреда и фандинга.
Требует pyqtgraph.
"""
from __future__ import annotations
import asyncio
import datetime

import pyqtgraph as pg
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QRectF
from PyQt5.QtGui import QPicture, QPainter, QColor, QPen, QBrush, QFont
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QTableWidget, QTableWidgetItem, QHeaderView,
    QSplitter, QFrame,
)

from core.history import (
    OHLCVBar, SpreadHistoryManager,
    fetch_historical_spread, fetch_funding_history,
    TF_SECONDS, funding_interval_label,
)

# ── Цвета ─────────────────────────────────────────────────────────────────────

_C = {
    'bg':     '#0d0f14',
    'bg2':    '#13161e',
    'bg3':    '#1a1e2a',
    'border': '#252a38',
    'text':   '#d6dae8',
    'muted':  '#5a607a',
    'accent': '#4f8ef7',
    'green':  '#3de8a0',
    'red':    '#f75f6e',
    'yellow': '#f7c94f',
}

pg.setConfigOptions(antialias=False)

MAX_SPREAD_RENDER_POINTS = 4_000
SPREAD_FETCH_TIMEOUT_SEC = 120

_SS = f"""
QMainWindow, QWidget {{
    background-color: {_C['bg']};
    color: {_C['text']};
    font-family: 'JetBrains Mono', 'Consolas', 'Courier New', monospace;
    font-size: 12px;
}}
QPushButton {{
    background-color: {_C['bg3']};
    color: {_C['text']};
    border: 1px solid {_C['border']};
    padding: 4px 10px;
    border-radius: 4px;
}}
QPushButton:hover {{ background-color: {_C['border']}; }}
QPushButton:checked {{
    background-color: {_C['accent']};
    color: #fff;
    border-color: {_C['accent']};
}}
QTableWidget {{
    background-color: {_C['bg2']};
    alternate-background-color: {_C['bg']};
    gridline-color: {_C['border']};
    color: {_C['text']};
    border: 1px solid {_C['border']};
}}
QHeaderView::section {{
    background-color: {_C['bg3']};
    color: {_C['muted']};
    padding: 4px 8px;
    border: none;
    border-bottom: 1px solid {_C['border']};
    font-size: 11px;
}}
QScrollBar:vertical {{
    background: {_C['bg']};
    width: 6px;
    border: none;
}}
QScrollBar::handle:vertical {{
    background: {_C['border']};
    border-radius: 3px;
    min-height: 20px;
}}
QSplitter::handle {{ background: {_C['border']}; width: 2px; height: 2px; }}
"""


def _dark_plot(plot: pg.PlotItem):
    plot.getViewBox().setBackgroundColor(_C['bg'])
    for axis_name in ('left', 'bottom', 'top', 'right'):
        ax = plot.getAxis(axis_name)
        if ax:
            ax.setPen(pg.mkPen(_C['border']))
            ax.setTextPen(pg.mkPen(_C['muted']))
    plot.getViewBox().setMenuEnabled(False)
    plot.hideButtons()


def _fmt_ts(ts_ms: int) -> str:
    return datetime.datetime.fromtimestamp(ts_ms / 1000).strftime('%d.%m %H:%M')


# ── Свечной элемент ───────────────────────────────────────────────────────────

class CandlestickItem(pg.GraphicsObject):
    def __init__(self):
        super().__init__()
        self._bars: list[OHLCVBar] = []
        self._tf_s = 60
        self._picture = QPicture()
        self._bounds  = QRectF()

    def set_data(self, bars: list[OHLCVBar], tf_s: int = 60):
        self._bars = bars
        self._tf_s = tf_s
        self._rebuild()
        self.prepareGeometryChange()
        self.update()

    def _rebuild(self):
        self._picture = QPicture()
        if not self._bars:
            self._bounds = QRectF()
            return

        p   = QPainter(self._picture)
        w   = self._tf_s * 0.38          # половина ширины тела в секундах
        UP  = QColor(_C['green'])
        DOWN = QColor(_C['red'])

        xs    = [b.ts / 1000 for b in self._bars]
        highs = [b.high for b in self._bars]
        lows  = [b.low  for b in self._bars]
        y_min, y_max = min(lows), max(highs)
        y_range = max(y_max - y_min, 1e-9)
        pad = y_range * 0.05

        self._bounds = QRectF(
            xs[0] - w, y_min - pad,
            xs[-1] - xs[0] + 2 * w, y_max - y_min + 2 * pad
        )

        for bar in self._bars:
            x   = bar.ts / 1000
            up  = bar.close >= bar.open
            col = UP if up else DOWN
            p.setPen(QPen(col, 1.0))
            p.setBrush(QBrush(col))

            # Фитиль
            p.drawLine(pg.Point(x, bar.low), pg.Point(x, bar.high))

            # Тело (минимальная высота — 0.1% диапазона графика)
            top = max(bar.open, bar.close)
            bot = min(bar.open, bar.close)
            h   = max(top - bot, y_range * 0.001)
            p.drawRect(QRectF(x - w, bot, 2 * w, h))

        p.end()

    def paint(self, p, *args):
        p.drawPicture(0, 0, self._picture)

    def boundingRect(self):
        return self._bounds


# ── Метки значений на значимых свечах ────────────────────────────────────────

def _add_candle_labels(
    plot: pg.PlotItem, bars: list[OHLCVBar], top_n: int = 6
) -> list[pg.TextItem]:
    """Добавляет текстовые метки на наиболее значимые свечи (по диапазону H-L)."""
    if not bars:
        return []
    ranked = sorted(bars, key=lambda b: abs(b.high - b.low), reverse=True)[:top_n]
    font   = QFont('Consolas', 8)
    added  = []
    for bar in ranked:
        x      = bar.ts / 1000
        is_pos = bar.close >= 0
        color  = _C['green'] if is_pos else _C['red']
        # Якорь (0.5, 1.0) → низ текста на точке → текст идёт вверх
        anchor = (0.5, 1.0) if is_pos else (0.5, 0.0)
        y_off  = abs(bar.high - bar.low) * 0.15
        y      = bar.high + y_off if is_pos else bar.low - y_off
        item   = pg.TextItem(text=f'{bar.close:+.3f}%', color=color, anchor=anchor)
        item.setFont(font)
        item.setPos(x, y)
        plot.addItem(item)
        added.append(item)
    return added


# ── Рабочие потоки для загрузки данных ────────────────────────────────────────

class SpreadFetchWorker(QThread):
    done  = pyqtSignal(list, list)
    error = pyqtSignal(str)

    def __init__(self, buy_exc_id, buy_mkt, sell_exc_id, sell_mkt, symbol, tf='1m'):
        super().__init__()
        self._args = (buy_exc_id, buy_mkt, sell_exc_id, sell_mkt, symbol, tf)

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            in_b, out_b = loop.run_until_complete(asyncio.wait_for(
                fetch_historical_spread(*self._args),
                timeout=SPREAD_FETCH_TIMEOUT_SEC,
            ))
            self.done.emit(in_b, out_b)
        except asyncio.TimeoutError:
            self.error.emit(f'Таймаут загрузки истории ({SPREAD_FETCH_TIMEOUT_SEC} сек)')
        except Exception as exc:
            self.error.emit(str(exc))
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()


class FundingFetchWorker(QThread):
    done  = pyqtSignal(list, list, str)
    error = pyqtSignal(str)

    def __init__(self, buy_exc_id, buy_mkt, sell_exc_id, sell_mkt, symbol):
        super().__init__()
        self._buy  = (buy_exc_id,  buy_mkt,  symbol)
        self._sell = (sell_exc_id, sell_mkt, symbol)

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def load_one(label: str, args: tuple):
                try:
                    return await fetch_funding_history(*args, strict=True), ""
                except Exception as exc:
                    return [], f"{label}: {exc}"

            async def load_both():
                return await asyncio.gather(
                    load_one("buy", self._buy),
                    load_one("sell", self._sell),
                )

            (b_h, b_err), (s_h, s_err) = loop.run_until_complete(load_both())
            warnings = " | ".join(err for err in (b_err, s_err) if err)
            self.done.emit(b_h, s_h, warnings)
        except Exception as exc:
            self.error.emit(str(exc))
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()


# ── Окно истории спреда ───────────────────────────────────────────────────────

class SpreadHistoryWindow(QMainWindow):
    """
    Показывает единый график исторического спреда из таблицы.
    """

    def __init__(
        self,
        pair_key: str,
        buy_label: str, sell_label: str,
        buy_exc_id: str, buy_mkt: str,
        sell_exc_id: str, sell_mkt: str,
        symbol: str,
        history: SpreadHistoryManager,
        parent=None,
    ):
        super().__init__(parent)
        self._pair_key  = pair_key
        self._buy_exc   = (buy_exc_id,  buy_mkt)
        self._sell_exc  = (sell_exc_id, sell_mkt)
        self._symbol    = symbol
        self._history   = history
        self._tf        = '1m'
        self._worker:   SpreadFetchWorker | None = None
        self._loaded_fetch_tfs: set[str] = set()
        self._pending_fetch_tf = ''
        self._fetch_completed = False

        self.setWindowTitle(f'История спреда — {symbol}  {buy_label} → {sell_label}')
        self.setStyleSheet(_SS)
        self.resize(1180, 720)

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(10, 8, 10, 8)
        root.setSpacing(5)

        # ── Верхняя панель ────────────────────────────────────────────────────
        top = QHBoxLayout()
        title_lbl = QLabel(f'<b>{symbol}</b>   {buy_label}  →  {sell_label}')
        title_lbl.setStyleSheet(f'color:{_C["accent"]}; font-size:14px;')
        top.addWidget(title_lbl)
        top.addStretch()
        self._status_lbl = QLabel('')
        self._status_lbl.setStyleSheet(f'color:{_C["muted"]}; font-size:11px;')
        top.addWidget(self._status_lbl)
        top.addSpacing(10)

        self._tf_btns: dict[str, QPushButton] = {}
        for tf in ('10s', '1m', '5m', '15m', '1h'):
            btn = QPushButton(tf)
            btn.setFixedSize(46, 26)
            btn.setCheckable(True)
            btn.setChecked(tf == '1m')
            btn.clicked.connect(lambda _checked, t=tf: self._switch_tf(t))
            self._tf_btns[tf] = btn
            top.addWidget(btn)

        top.addSpacing(6)
        refresh_btn = QPushButton('↺ Обновить')
        refresh_btn.setFixedHeight(26)
        refresh_btn.clicked.connect(self._fetch_historical)
        self._refresh_btn = refresh_btn
        top.addWidget(refresh_btn)
        root.addLayout(top)

        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet(f'background:{_C["border"]};')
        sep.setFixedHeight(1)
        root.addWidget(sep)

        # ── Графики (pyqtgraph) ───────────────────────────────────────────────
        self._glw = pg.GraphicsLayoutWidget()
        self._glw.setBackground(_C['bg'])
        root.addWidget(self._glw, 1)

        self._plot = self._glw.addPlot(
            row=0, col=0,
            axisItems={'bottom': pg.DateAxisItem()},
        )
        self._plot.showGrid(x=True, y=True, alpha=0.15)
        self._plot.setLabel('left', 'Spread %', color=_C['muted'])
        _dark_plot(self._plot)

        self._curve_close = pg.PlotDataItem(
            [], [],
            pen=pg.mkPen('#72b7ff', width=1),
            connect='finite',
        )
        self._curve_close.setClipToView(True)
        self._curve_close.setDownsampling(auto=True, method='subsample')
        self._plot.addItem(self._curve_close)
        self._plot.addItem(pg.InfiniteLine(
            angle=0, pos=0,
            pen=pg.mkPen(_C['muted'], width=1, style=Qt.DashLine),
        ))

        # ── Автообновление live-данных каждые 15 сек ──────────────────────────
        self._live_timer = QTimer(self)
        self._live_timer.setInterval(15_000)
        self._live_timer.timeout.connect(lambda: self._render(auto_range=False))
        self._live_timer.start()

        # ── Начальный рендер + загрузка истории ──────────────────────────────
        self._render(auto_range=True)
        self._fetch_historical()

    # ── Переключение таймфрейма ───────────────────────────────────────────────

    def _switch_tf(self, tf: str):
        self._tf = tf
        for t, btn in self._tf_btns.items():
            btn.setChecked(t == tf)
        self._render(auto_range=True)
        fetch_tf = self._fetch_tf_for_current_view()
        if fetch_tf not in self._loaded_fetch_tfs:
            self._fetch_historical()

    # ── Загрузка исторических данных ─────────────────────────────────────────

    def _fetch_historical(self):
        if self._worker and self._worker.isRunning():
            return
        fetch_tf = self._fetch_tf_for_current_view()
        self._pending_fetch_tf = fetch_tf
        self._fetch_completed = False
        self._status_lbl.setText('Загрузка исторических данных…')
        self._status_lbl.setToolTip('')
        self._refresh_btn.setEnabled(False)
        self._refresh_btn.setText('Загрузка…')
        worker = SpreadFetchWorker(*self._buy_exc, *self._sell_exc, self._symbol, fetch_tf)
        self._worker = worker
        worker.done.connect(lambda in_b, out_b, w=worker: self._on_fetch_done(in_b, out_b, w))
        worker.error.connect(lambda msg, w=worker: self._on_fetch_error(msg, w))
        worker.finished.connect(lambda w=worker: self._on_fetch_finished(w))
        worker.start()

    def _on_fetch_done(self, in_bars: list, out_bars: list, worker=None):
        if worker is not None and worker is not self._worker:
            return
        self._fetch_completed = True
        self._loaded_fetch_tfs.add(self._pending_fetch_tf or self._fetch_tf_for_current_view())
        self._history.set_historical(self._pair_key, in_bars, out_bars)
        loaded_tf = f'{int(in_bars[0].interval_ms / 1000)}s' if in_bars else self._fetch_tf_for_current_view()
        if in_bars and in_bars[0].interval_ms >= 60_000:
            loaded_tf = f'{int(in_bars[0].interval_ms / 60_000)}m'
        self._status_lbl.setText(f'Загружено {len(in_bars)} свечей ({loaded_tf})  •  live ↻')
        self._status_lbl.setToolTip('')
        self._refresh_btn.setEnabled(True)
        self._refresh_btn.setText('↺ Обновить')
        self._render(auto_range=True)

    def _on_fetch_error(self, msg: str, worker=None):
        if worker is not None and worker is not self._worker:
            return
        self._fetch_completed = True
        self._render(auto_range=True)
        self._status_lbl.setText(f'Ошибка загрузки: {msg[:140]}  (показаны live-данные)')
        self._status_lbl.setToolTip(msg)
        self._refresh_btn.setEnabled(True)
        self._refresh_btn.setText('↺ Обновить')

    def _on_fetch_finished(self, worker=None):
        if worker is not None and worker is not self._worker:
            return
        if not self._fetch_completed:
            self._render(auto_range=True)
            self._status_lbl.setText('Ошибка загрузки: поток завершился без результата  (показаны live-данные)')
            self._status_lbl.setToolTip('Поток загрузки исторических данных завершился без сигналов done/error.')
        self._refresh_btn.setEnabled(True)
        self._refresh_btn.setText('↺ Обновить')
        self._fetch_completed = False
        self._worker = None

    # ── Отрисовка графиков ────────────────────────────────────────────────────

    def _fetch_tf_for_current_view(self) -> str:
        return '10s' if self._tf == '10s' else '1m'

    def _render(self, auto_range: bool = False):
        in_bars, _out_bars = self._history.get_bars(self._pair_key, self._tf)
        if not in_bars:
            self._curve_close.setData([], [])
            return

        total_bars = len(in_bars)
        render_bars = self._bars_for_current_view(in_bars, auto_range)
        xs = [bar.ts / 1000 for bar in render_bars]
        closes = [bar.close for bar in render_bars]
        self._curve_close.setData(xs, closes)
        if auto_range:
            self._plot.autoRange()
        self._status_lbl.setText(
            f'{total_bars} точек  •  последний {in_bars[-1].close:+.3f}%  •  live ↻'
        )

    def _bars_for_current_view(self, bars: list[OHLCVBar], auto_range: bool) -> list[OHLCVBar]:
        if len(bars) <= MAX_SPREAD_RENDER_POINTS:
            return bars

        if not auto_range:
            x_min, x_max = self._plot.vb.viewRange()[0]
            left = int(x_min * 1000)
            right = int(x_max * 1000)
            visible = [
                bar for bar in bars
                if left <= bar.ts <= right
            ]
            if visible:
                bars = visible

        if len(bars) <= MAX_SPREAD_RENDER_POINTS:
            return bars

        step = max(1, len(bars) // MAX_SPREAD_RENDER_POINTS)
        thinned = bars[::step]
        if thinned[-1].ts != bars[-1].ts:
            thinned.append(bars[-1])
        return thinned

    def closeEvent(self, event):
        self._live_timer.stop()
        if self._worker and self._worker.isRunning():
            self._worker.quit()
            self._worker.wait(2000)
        super().closeEvent(event)


# ── Окно истории фандинга ─────────────────────────────────────────────────────

class FundingHistoryWindow(QMainWindow):
    """
    Показывает историю ставок фандинга:
      • Линейный график с двумя сериями (buy / sell биржи)
      • Таблица с числовыми значениями
    Для SPOT-биржи показывается прочерк.
    """

    def __init__(
        self,
        buy_label: str, sell_label: str,
        buy_exc_id: str, buy_mkt: str,
        sell_exc_id: str, sell_mkt: str,
        symbol: str,
        parent=None,
    ):
        super().__init__(parent)
        self._buy_mkt  = buy_mkt
        self._sell_mkt = sell_mkt
        self._worker: FundingFetchWorker | None = None

        self.setWindowTitle(f'История фандинга — {symbol}  {buy_label} / {sell_label}')
        self.setStyleSheet(_SS)
        self.resize(1000, 620)

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(10, 8, 10, 8)
        root.setSpacing(5)

        # ── Заголовок ─────────────────────────────────────────────────────────
        hdr = QHBoxLayout()
        title = QLabel(f'<b>Фандинг:</b>  {symbol}   {buy_label}  /  {sell_label}')
        title.setStyleSheet(f'color:{_C["accent"]}; font-size:14px;')
        hdr.addWidget(title)
        hdr.addStretch()
        self._status_lbl = QLabel('Загрузка…')
        self._status_lbl.setStyleSheet(f'color:{_C["muted"]}; font-size:11px;')
        hdr.addWidget(self._status_lbl)
        root.addLayout(hdr)

        # ── Легенда ───────────────────────────────────────────────────────────
        leg = QHBoxLayout()
        for color, text in [
            (_C['accent'],  f'● {buy_label}'),
            (_C['yellow'], f'● {sell_label}'),
            (_C['red'],    '  +% LONG: платим'),
            (_C['green'],  '  −% LONG: получаем'),
            (_C['green'],  '  +% SHORT: получаем'),
            (_C['red'],    '  −% SHORT: платим'),
        ]:
            lbl = QLabel(text)
            lbl.setStyleSheet(f'color:{color}; font-size:10px;')
            leg.addWidget(lbl)
        leg.addStretch()
        root.addLayout(leg)

        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setStyleSheet(f'background:{_C["border"]};')
        sep.setFixedHeight(1)
        root.addWidget(sep)

        # ── Сплиттер: график + таблица ────────────────────────────────────────
        splitter = QSplitter(Qt.Vertical)

        # График
        chart = pg.PlotWidget(
            axisItems={'bottom': pg.DateAxisItem()},
            background=_C['bg'],
        )
        self._plot = chart.getPlotItem()
        self._plot.showGrid(x=True, y=True, alpha=0.15)
        self._plot.setLabel('left', 'Фандинг %', color=_C['muted'])
        self._plot.addLegend(
            offset=(10, 10),
            labelTextColor=_C['text'],
            pen=pg.mkPen(_C['border']),
            brush=pg.mkBrush(_C['bg2']),
        )
        _dark_plot(self._plot)

        buy_color  = _C['accent'] if buy_mkt  == 'perp' else _C['muted']
        sell_color = _C['yellow'] if sell_mkt == 'perp' else _C['muted']

        buy_name  = buy_label  if buy_mkt  == 'perp' else f'{buy_label} (SPOT)'
        sell_name = sell_label if sell_mkt == 'perp' else f'{sell_label} (SPOT)'

        self._curve_buy  = self._plot.plot(
            pen=pg.mkPen(buy_color,  width=2), name=buy_name,
            symbol='o', symbolSize=5, symbolBrush=buy_color, symbolPen=None,
        )
        self._curve_sell = self._plot.plot(
            pen=pg.mkPen(sell_color, width=2), name=sell_name,
            symbol='o', symbolSize=5, symbolBrush=sell_color, symbolPen=None,
        )

        zero = pg.InfiniteLine(
            angle=0, pos=0,
            pen=pg.mkPen(_C['muted'], width=1, style=Qt.DashLine)
        )
        self._plot.addItem(zero)
        splitter.addWidget(chart)

        # Таблица
        col2 = buy_label  if buy_mkt  == 'perp' else f'{buy_label} —'
        col3 = sell_label if sell_mkt == 'perp' else f'{sell_label} —'
        self._table = QTableWidget(0, 3)
        self._table.setHorizontalHeaderLabels(['Время', col2, col3])
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setAlternatingRowColors(True)
        hdr_view = self._table.horizontalHeader()
        hdr_view.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hdr_view.setSectionResizeMode(1, QHeaderView.Stretch)
        hdr_view.setSectionResizeMode(2, QHeaderView.Stretch)
        self._table.verticalHeader().setDefaultSectionSize(24)
        splitter.addWidget(self._table)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        root.addWidget(splitter, 1)

        # ── Загрузка ──────────────────────────────────────────────────────────
        self._worker = FundingFetchWorker(
            buy_exc_id, buy_mkt, sell_exc_id, sell_mkt, symbol
        )
        self._worker.done.connect(self._on_done)
        self._worker.error.connect(self._on_error)
        self._worker.start()

    def _on_done(self, buy_hist: list, sell_hist: list, warnings: str = ""):
        b_cnt = len(buy_hist)
        s_cnt = len(sell_hist)
        b_int = funding_interval_label(buy_hist) if self._buy_mkt == 'perp' else 'spot'
        s_int = funding_interval_label(sell_hist) if self._sell_mkt == 'perp' else 'spot'
        text = f'Загружено: buy={b_cnt} ({b_int})  /  sell={s_cnt} ({s_int})'
        if warnings:
            text += f'  |  Ошибки: {warnings[:90]}'
            self._status_lbl.setToolTip(warnings)
        else:
            self._status_lbl.setToolTip('')
        self._status_lbl.setText(text)
        self._draw(buy_hist, sell_hist)

    def _on_error(self, msg: str):
        self._status_lbl.setText(f'Ошибка: {msg[:80]}')

    def _draw(self, buy_hist: list, sell_hist: list):
        def to_xy(hist: list):
            if not hist:
                return [], []
            return [r['ts'] / 1000 for r in hist], [r['rate'] for r in hist]

        bx, by = to_xy(buy_hist)
        sx, sy = to_xy(sell_hist)

        self._curve_buy.setData( bx, by) if self._buy_mkt  == 'perp' and bx else self._curve_buy.setData([], [])
        self._curve_sell.setData(sx, sy) if self._sell_mkt == 'perp' and sx else self._curve_sell.setData([], [])

        # Таблица: объединяем по временным меткам, сортируем по убыванию
        buy_map  = {r['ts']: r['rate'] for r in buy_hist}  if buy_hist  else {}
        sell_map = {r['ts']: r['rate'] for r in sell_hist} if sell_hist else {}
        all_ts   = sorted(set(buy_map) | set(sell_map), reverse=True)

        self._table.setRowCount(0)
        for ts in all_ts:
            row = self._table.rowCount()
            self._table.insertRow(row)

            ts_item = QTableWidgetItem(_fmt_ts(ts))
            ts_item.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
            ts_item.setForeground(QColor(_C['muted']))
            self._table.setItem(row, 0, ts_item)

            self._table.setItem(row, 1, _rate_cell(buy_map.get(ts),  self._buy_mkt,  'long'))
            self._table.setItem(row, 2, _rate_cell(sell_map.get(ts), self._sell_mkt, 'short'))

    def closeEvent(self, event):
        if self._worker and self._worker.isRunning():
            self._worker.quit()
            self._worker.wait(2000)
        super().closeEvent(event)


# ── Вспомогательная функция таблицы ──────────────────────────────────────────

def _rate_cell(rate: float | None, mkt: str, side: str) -> QTableWidgetItem:
    it = QTableWidgetItem()
    it.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
    if mkt == 'spot' or rate is None:
        it.setText('—')
        it.setForeground(QColor(_C['muted']))
    else:
        it.setText(f'{rate:+.4f}%')
        is_good = rate < 0 if side == 'long' else rate > 0
        if rate == 0:
            it.setForeground(QColor(_C['muted']))
        else:
            it.setForeground(QColor(_C['green'] if is_good else _C['red']))
    return it
