from typing import Any

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, SymbolSet, Symbol

TIMEFRAME = {
    TimeFrameEnum.MINUTE: '1m',
    TimeFrameEnum.HOUR: '1h',
    TimeFrameEnum.DAY: '1D',
}

class Exchange(BaseExchange):
    API_BASE_PATH = 'https://api-pub.bitfinex.com/v2/'
    EXCHANGE_ID = ExchangeEnum.BITFINEX
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.BITFINEX, symbol_type=SymbolTypeEnum.SPOT, native_id='BTCUSD'),
        Symbol(exchange_id=ExchangeEnum.BITFINEX, symbol_type=SymbolTypeEnum.PERP_USD, native_id='BTCF0:USTF0'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://docs.bitfinex.com/reference#rest-public-candles
        endpoint = f'/candles/trade:{TIMEFRAME[timeframe]}:t{symbol.native_id}/hist'
        params = {
            'limit': 1000,                  # MAX 1000
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        labels = ('period', 'open', 'close', 'high', 'low')
        return [OHLC.from_list(r[:5], labels) for r in response]
