import datetime as dt
from typing import Any

import pendulum
from devtools import debug
from pydantic import Field, parse_obj_as

from src.enums import ExchangeEnum, SymbolTypeEnum, TimeFrameEnum
from src.exchanges.base import BaseExchange
from src.models import OHLC, FundingRate, SymbolSet, Symbol
from src.models.funding_rate import RunningFundingRate

TIMEFRAME = {
    TimeFrameEnum.MINUTE: '1m',
    TimeFrameEnum.HOUR: '1H',
    TimeFrameEnum.DAY: '1Dutc'
}


class OkexFundingRate(FundingRate):
    timestamp: dt.datetime = Field(alias='fundingTime')
    funding_rate: float = Field(alias='realizedRate')


class Exchange(BaseExchange):
    # https://www.okx.com/docs-v5/en/
    API_BASE_PATH = 'https://www.okx.com/'
    EXCHANGE_ID = ExchangeEnum.OKEX
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.OKEX, symbol_type=SymbolTypeEnum.SPOT, native_id='BTC-USDT'),
        Symbol(exchange_id=ExchangeEnum.OKEX, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='BTC-USD-SWAP'),
        Symbol(exchange_id=ExchangeEnum.OKEX, symbol_type=SymbolTypeEnum.PERP_USD, native_id='BTC-USDT-SWAP'),
    ])

    #############
    # OHLC
    #############
    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://www.okx.com/docs-v5/en/#rest-api-market-data-get-candlesticks
        endpoint = '/api/v5/market/candles'
        params = {
            'instId': symbol.native_id,
            'bar': TIMEFRAME[timeframe],
            'limit': 300,  # max 300, default 100
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        return [OHLC.from_list(r[:5]) for r in response['data']]

    #############
    # Funding
    #############
    @classmethod
    async def _fetch_funding(cls, symbol: Symbol) -> list[FundingRate]:
        # https://www.okx.com/docs-v5/en/#rest-api-public-data-get-funding-rate-history
        endpoint = '/api/v5/public/funding-rate-history'
        params = {
            'instId': symbol.native_id,
            'limit': 100,  # max 100, default 100
        }
        return await cls._fetch_endpoint(endpoint, params=params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[OkexFundingRate]:
        return parse_obj_as(list[OkexFundingRate], response['data'])

    ##############
    # Running funding
    ##############
    @classmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        # https://www.okx.com/docs-v5/en/#rest-api-public-data-get-funding-rate
        endpoint = '/api/v5/public/funding-rate'
        params = {'instId': symbol.native_id}
        return await cls._fetch_endpoint(endpoint, params=params)

    @staticmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        result = response['data'].pop()
        #debug(result)
        return RunningFundingRate(
            timestamp=pendulum.now('UTC'),
            funding_rate=result['fundingRate'],
            funding_timestamp=result['fundingTime'],
            predicted_funding_rate=result['nextFundingRate']
        )
