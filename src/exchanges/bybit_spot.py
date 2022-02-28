from typing import Any

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, Symbol, SymbolSet

TIMEFRAME = {
    TimeFrameEnum.MINUTE: '1m',
    TimeFrameEnum.HOUR: '1h',
    TimeFrameEnum.DAY: '1d',
}

class Exchange(BaseExchange):
    API_BASE_PATH = 'https://api.bybit.com/'
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.BYBIT, symbol_type=SymbolTypeEnum.SPOT, native_id='BTCUSDT'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://bybit-exchange.github.io/docs/spot/#t-querykline
        endpoint = '/spot/quote/v1/kline'
        params = {
            'symbol': symbol.native_id,
            'interval': TIMEFRAME[timeframe],
            'limit': 1000,  # MAX 1000
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        return [OHLC.from_list(r[:5]) for r in response['result']]



