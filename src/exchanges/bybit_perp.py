import datetime as dt
from typing import Any

import pendulum
from devtools import debug
from pydantic import Field, parse_obj_as, NonNegativeFloat

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum, AssetEnum
from src.models import OHLC, SymbolSet, Symbol, FundingRate
from src.models.funding_rate import RunningFundingRate


class BybitOhlc(OHLC):
    period: dt.datetime = Field(alias='open_time')

class BybitFundingRate(FundingRate):
    timestamp: dt.datetime = Field(alias='funding_rate_timestamp')
    funding_rate: float


class Exchange(BaseExchange):
    API_BASE_PATH = 'https://api.bybit.com/'
    EXCHANGE_ID = ExchangeEnum.BYBIT
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.BYBIT, symbol_type=SymbolTypeEnum.PERP_USD, native_id='BTCUSDT'),
        Symbol(exchange_id=ExchangeEnum.BYBIT, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='BTCUSD'),
    ])

    ############
    # OHLC
    ############
    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://bybit-exchange.github.io/docs/inverse/#t-querykline
        # https://bybit-exchange.github.io/docs/linear/#t-querykline
        inverse_endpoint = '/v2/public/kline/list'
        linear_endpoint = '/public/linear/kline'
        endpoint = linear_endpoint if symbol.margin == AssetEnum.USD else inverse_endpoint
        limit = 2
        params = {
            'symbol': symbol.native_id,
            'interval': timeframe.value if timeframe.value < timeframe.DAY.value else 'D', # anything above 1D need hack
            'from': int(pendulum.now('UTC').subtract(minutes=limit*timeframe.value).timestamp()),
            'limit': limit,  # MAX 200
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[BybitOhlc]:
        return parse_obj_as(list[BybitOhlc], response['result'])

    ############
    # Funding
    ############
    @classmethod
    async def _fetch_funding(cls, symbol: Symbol):
        # https://bybit-exchange.github.io/docs/inverse/#t-funding
        # https://bybit-exchange.github.io/docs/linear/#t-fundingrate
        params = {'symbol': symbol.native_id}
        inverse_endpoint = '/v2/public/funding/prev-funding-rate'
        linear_endpoint = '/public/linear/funding/prev-funding-rate'
        endpoint = linear_endpoint if symbol.margin == AssetEnum.USD else inverse_endpoint
        return [await cls._fetch_endpoint(endpoint, params=params)]


    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[BybitFundingRate]:
        return [parse_obj_as(BybitFundingRate, response[0]['result'])]

    ############
    # Runing funding
    ############
    @classmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        # https://bybit-exchange.github.io/docs/inverse/?console#t-latestsymbolinfo
        response = await cls._fetch_endpoint('/v2/public/tickers')
        #debug(response)
        for record in response['result']:
            if record['symbol'] == symbol.native_id:
                return record

    @staticmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        return RunningFundingRate(
            timestamp=pendulum.now('UTC'),
            funding_rate=response['funding_rate'],
            predicted_funding_rate=response['predicted_funding_rate']
        )

