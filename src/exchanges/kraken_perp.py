import datetime as dt
from typing import Any

import pendulum
from devtools import debug
from pydantic import Field, parse_obj_as

from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.exchanges.base import BaseExchange
from src.models import OHLC, SymbolSet, Symbol, FundingRate
from src.models.funding_rate import RunningFundingRate

TIMEFRAME = {
    TimeFrameEnum.MINUTE: '1m',
    TimeFrameEnum.HOUR: '1h',
    TimeFrameEnum.DAY: '1d',
}


class KrakenOhlc(OHLC):
    period: dt.datetime = Field(alias='time')

class KrakenFundingRate(FundingRate):
    funding_rate: float = Field(alias='relativeFundingRate')


class Exchange(BaseExchange):
    API_BASE_PATH = 'https://futures.kraken.com/'
    EXCHANGE_ID = ExchangeEnum.KRAKEN
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.KRAKEN, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='PI_XBTUSD'),
    ])

    #############
    # OHLC
    #############
    @classmethod
    async def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://support.kraken.com/hc/en-us/articles/4403284627220-OHLC
        endpoint = f'api/charts/v1/trade/{symbol.native_id}/{TIMEFRAME[timeframe]}'
        params = {
            'from': pendulum.now('UTC').subtract(minutes=1000 * timeframe.value).int_timestamp
        }
        return await cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[KrakenOhlc]:
        # debug(response)
        return parse_obj_as(list[KrakenOhlc], response['candles'])

    #############
    # Funding
    #############
    @classmethod
    async def _fetch_funding(cls, symbol: Symbol):
        # https://support.kraken.com/hc/en-us/articles/360061979852-Historical-Funding-Rates
        endpoint = 'derivatives/api/v4/historicalfundingrates'
        params = {
            'symbol': symbol.native_id
        }
        return await cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[KrakenFundingRate]:
        return parse_obj_as(list[KrakenFundingRate], response['rates'])

    ###############
    # Running funding
    ###############
    @classmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        # https://support.kraken.com/hc/en-us/articles/360022839531-Tickers
        endpoint = 'derivatives/api/v3/tickers'
        response = await cls._fetch_endpoint(endpoint)
        for record in response['tickers']:
            if record['symbol'].upper() == symbol.native_id:
                return record

    @staticmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        # debug(response)
        scale_factor = 1_000_000
        return RunningFundingRate(
            timestamp=pendulum.now('UTC'),
            funding_rate=response['fundingRate'] * scale_factor,
            predicted_funding_rate=response['fundingRatePrediction'] * scale_factor,
            funding_timestamp = pendulum.now('UTC') #TODO: get correct time
        )