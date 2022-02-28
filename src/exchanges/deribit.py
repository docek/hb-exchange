import datetime as dt
from typing import Any

import pendulum
from devtools import debug
from pydantic import NonNegativeFloat, Field, parse_obj_as

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, FundingRate, SymbolSet, Symbol


class DeribitFundingRate(FundingRate):
    timestamp: dt.datetime
    prev_index_price: NonNegativeFloat
    interest_8h: float
    funding_rate: float = Field(alias='interest_1h')
    index_price: NonNegativeFloat


class Exchange(BaseExchange):
    API_BASE_PATH = 'https://deribit.com/api/v2'
    EXCHANGE_ID = ExchangeEnum.DERIBIT
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.DERIBIT, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='BTC-PERPETUAL'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://docs.deribit.com/#public-get_tradingview_chart_data
        now = pendulum.now('UTC')
        endpoint = f'/public/get_tradingview_chart_data'
        params = {
            'instrument_name': symbol.native_id,
            'resolution': timeframe.value,  # resolution in minutes
            'start_timestamp': 1000 * now.subtract(minutes=1000 * timeframe.value).int_timestamp,
            'end_timestamp': 1000 * now.int_timestamp
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        result = response['result']
        n = len(result['ticks'])
        return [OHLC(
            period=result['ticks'][i],
            open=result['open'][i],
            high=result['high'][i],
            low=result['low'][i],
            close=result['close'][i]
        ) for i in range(n)]

    @classmethod
    async def _fetch_funding(cls, symbol: Symbol) -> list[FundingRate]:
        # https://docs.deribit.com/#public-get_funding_rate_history
        endpoint = '/public/get_funding_rate_history'
        now = pendulum.now('UTC')
        params = {
            'instrument_name': symbol.native_id,
            'start_timestamp': int(1000 * now.subtract(days=28).timestamp()),
            'end_timestamp': int(1000 * now.timestamp())
        }
        return await cls._fetch_endpoint(endpoint, params=params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[DeribitFundingRate]:
        return parse_obj_as(list[DeribitFundingRate], response['result'])