import datetime as dt
from typing import Any

from devtools import debug
from pydantic import Field, parse_obj_as

from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.exchanges.base import BaseExchange
from src.models import OHLC, FundingRate, SymbolSet, Symbol
from src.models.funding_rate import RunningFundingRate

TIMEFRAME = {
    TimeFrameEnum.MINUTE: '1m',
    TimeFrameEnum.HOUR: '1h',
    TimeFrameEnum.DAY: '1d',
}


class BitmexFundingRate(FundingRate):
    funding_rate: float = Field(alias='fundingRate')


class BitmexOhlc(OHLC):
    period: dt.datetime = Field(alias='timestamp')


class Exchange(BaseExchange):
    API_BASE_PATH = 'https://www.bitmex.com/api/v1'
    EXCHANGE_ID = ExchangeEnum.BITMEX
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.BITMEX, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='XBT'),
        Symbol(exchange_id=ExchangeEnum.BITMEX, symbol_type=SymbolTypeEnum.PERP_USD, native_id='XBTUSDT'),
    ])

    ############
    # OHLC
    ############
    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://www.bitmex.com/api/explorer/#!/Trade/Trade_getBucketed
        endpoint = '/trade/bucketed/'
        params = {
            'binSize': TIMEFRAME[timeframe],
            'symbol': symbol.native_id,
            'partial': True,  # True to fetch incomplete last period
            'reverse': True,  # to get rid of start and end time and rather fetch default
            'count': 1000,
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[BitmexOhlc]:
        return parse_obj_as(list[BitmexOhlc], response)

    @staticmethod
    def _ohlc_fix(parsed_ohlc: list[OHLC], symbol: Symbol, timeframe: TimeFrameEnum) -> list[OHLC]:
        for o in parsed_ohlc:
            o.period = o.period - dt.timedelta(minutes=timeframe.value)
        return parsed_ohlc

    ############
    # Funding
    ############
    @classmethod
    async def _fetch_funding(cls, symbol: Symbol):
        # https://www.bitmex.com/api/explorer/#!/Funding/Funding_get
        params = {
            'symbol': symbol.native_id,
            'count': 100,
            'start': 0,
            'reverse': True
        }
        return await cls._fetch_endpoint('/funding', params=params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[BitmexFundingRate]:
        return parse_obj_as(list[BitmexFundingRate], response)

    #############
    # Running funding
    #############
    @classmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        # https://www.bitmex.com/api/explorer/#!/Instrument/Instrument_get
        params = {'symbol': symbol.native_id}
        return await cls._fetch_endpoint('/instrument', params=params)

    @staticmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        result = response.pop()
        # debug(result)
        return RunningFundingRate(
            timestamp = result['timestamp'],
            funding_timestamp = result['fundingTimestamp'],
            funding_rate = result['fundingRate'],
            predicted_funding_rate = result['indicativeFundingRate']
        )
