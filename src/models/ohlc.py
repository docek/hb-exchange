from __future__ import annotations

import datetime as dt
from typing import Optional

from pydantic import BaseModel

from .primitives import PriceQuote


class OHLC(BaseModel):
    period: dt.datetime
    open: PriceQuote
    high: PriceQuote
    low: PriceQuote
    close: PriceQuote

    @staticmethod
    def from_list(record: list[int|float], labels: Optional[tuple[str, str, str, str, str]] = None) -> OHLC:
        labels = ('period', 'open', 'high', 'low', 'close') if not labels else labels
        return OHLC(**{key: value for key, value in zip(labels, record)})