from __future__ import annotations

import os
import sqlite3
import threading
from typing import Iterable

from core.config import get_history_db_path
from core.history_types import OHLCVBar


_DB_LOCK = threading.Lock()


class SpreadHistoryStorage:
    def __init__(self, path: str | None = None):
        self.path = path or get_history_db_path()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _ensure_schema(self) -> None:
        with _DB_LOCK:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS spread_bars (
                        symbol TEXT NOT NULL,
                        pair_key TEXT NOT NULL,
                        side TEXT NOT NULL,
                        ts INTEGER NOT NULL,
                        interval_ms INTEGER NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now') * 1000),
                        PRIMARY KEY (symbol, pair_key, side, ts, interval_ms)
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_spread_bars_lookup "
                    "ON spread_bars(symbol, pair_key, side, ts)"
                )

    def save_bars(self, symbol: str, pair_key: str, side: str, bars: Iterable[OHLCVBar]) -> None:
        rows = [
            (
                symbol.upper(),
                pair_key,
                side,
                int(bar.ts),
                int(bar.interval_ms),
                float(bar.open),
                float(bar.high),
                float(bar.low),
                float(bar.close),
            )
            for bar in bars
        ]
        if not rows:
            return

        with _DB_LOCK:
            with self._connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO spread_bars
                        (symbol, pair_key, side, ts, interval_ms, open, high, low, close)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, pair_key, side, ts, interval_ms)
                    DO UPDATE SET
                        open=excluded.open,
                        high=excluded.high,
                        low=excluded.low,
                        close=excluded.close,
                        updated_at=(strftime('%s','now') * 1000)
                    """,
                    rows,
                )

    def load_bars(self, symbol: str, pair_key: str, side: str, limit: int = 50_000) -> list[OHLCVBar]:
        with _DB_LOCK:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT ts, open, high, low, close, interval_ms
                    FROM spread_bars
                    WHERE symbol = ? AND pair_key = ? AND side = ?
                    ORDER BY ts DESC, interval_ms DESC
                    LIMIT ?
                    """,
                    (symbol.upper(), pair_key, side, limit),
                ).fetchall()
        return [
            OHLCVBar(int(ts), float(o), float(h), float(l), float(c), int(interval_ms))
            for ts, o, h, l, c, interval_ms in reversed(rows)
        ]
