from typing import Any

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, SymbolSet, Symbol


class Exchange(BaseExchange):
    API_BASE_PATH = 'https://api.exchange.coinbase.com/'
    EXCHANGE_ID = ExchangeEnum.COINBASE
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.COINBASE, symbol_type=SymbolTypeEnum.SPOT, native_id='BTC-USD'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles
        endpoint = f'/products/{symbol.native_id}/candles'
        params = {
            'granularity': timeframe.value * 60,   # resolution in seconds
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        return [OHLC.from_list(r[:5]) for r in response]
