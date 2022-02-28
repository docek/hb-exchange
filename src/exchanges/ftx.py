import datetime as dt
from typing import Any

from devtools import debug
from pydantic import Field, parse_obj_as, NonNegativeFloat

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, SymbolSet, Symbol, FundingRate


class FtxOhlc(OHLC):
    period: dt.datetime = Field(alias='startTime')

class FtxFundingRate(FundingRate):
    timestamp: dt.datetime = Field(alias='time')
    funding_rate: float = Field(alias='rate')

class Exchange(BaseExchange):
    API_BASE_PATH = 'https://ftx.com/api/'
    EXCHANGE_ID = ExchangeEnum.FTX
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.FTX, symbol_type=SymbolTypeEnum.SPOT, native_id='BTC/USD'),
        Symbol(exchange_id=ExchangeEnum.FTX, symbol_type=SymbolTypeEnum.PERP_USD, native_id='BTC-PERP'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://docs.ftx.com/?python#get-historical-prices
        endpoint = f'/markets/{symbol.native_id}/candles'
        params = {
            'resolution': timeframe.value * 60,   # resolution in seconds
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[FtxOhlc]:
        return parse_obj_as(list[FtxOhlc], response['result'])


    @classmethod
    async def _fetch_funding(cls, symbol: Symbol):
        # https://docs.ftx.com/#get-funding-rates
        endpoint = '/funding_rates'
        params = {
            'future': symbol.native_id,
        }
        return await cls._fetch_endpoint(endpoint, params=params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[FtxFundingRate]:

        return parse_obj_as(list[FtxFundingRate], response['result'])
