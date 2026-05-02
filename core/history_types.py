from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OHLCVBar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    interval_ms: int = 60_000
