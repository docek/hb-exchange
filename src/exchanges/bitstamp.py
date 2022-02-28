import datetime as dt
from typing import Any

from pydantic import Field, parse_obj_as, NonNegativeFloat

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, SymbolSet, Symbol


class BitstampOhlc(OHLC):
    period: dt.datetime = Field(alias='timestamp')

class Exchange(BaseExchange):
    API_BASE_PATH = 'https://www.bitstamp.net/api/v2/'
    EXCHANGE_ID = ExchangeEnum.BITSTAMP
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.BITSTAMP, symbol_type=SymbolTypeEnum.SPOT, native_id='btcusd'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://www.bitstamp.net/api/#ohlc_data
        endpoint = f'/ohlc/{symbol.native_id}/'
        params = {
            'step': timeframe.value * 60,   # resolution in seconds
            'limit': 1000,                  # MAX 1000
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[BitstampOhlc]:
        return parse_obj_as(list[BitstampOhlc], response['data']['ohlc'])
