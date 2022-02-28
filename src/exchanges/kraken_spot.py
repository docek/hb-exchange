from typing import Any

from devtools import debug

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, SymbolSet, Symbol


class Exchange(BaseExchange):
    API_BASE_PATH = 'https://api.kraken.com/0/'
    EXCHANGE_ID = ExchangeEnum.KRAKEN
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.KRAKEN, symbol_type=SymbolTypeEnum.SPOT, native_id='XXBTZUSD'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://docs.kraken.com/rest/#operation/getOHLCData
        endpoint = '/public/OHLC'
        params = {
            'pair': symbol.native_id,
            'interval': timeframe.value,        # resolution in minutes
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        #return [OHLC.from_list(r, labels) for r in response['result']['XXBTZUSD']]
        return [OHLC.from_list(r[:5]) for r in list(response['result'].values())[0]]