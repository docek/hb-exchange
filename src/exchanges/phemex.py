from typing import Any

import datetime as dt
import pendulum
from devtools import debug
from loguru import logger
from pydantic import validator, Field, parse_obj_as

from src.exchanges.base import BaseExchange
from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, SymbolSet, Symbol, FundingRate
from src.models.funding_rate import RunningFundingRate

SCALE_FACTOR = 10_000


class PhemexFundingRate(FundingRate):
    timestamp: dt.datetime = Field(alias='createdAt')
    funding_rate: float = Field(alias='rateEr')

    @validator('funding_rate')
    def rescale_rate(cls, v) -> float:
        return v / SCALE_FACTOR ** 2


class Exchange(BaseExchange):
    API_BASE_PATH = 'https://api.phemex.com/'
    EXCHANGE_ID = ExchangeEnum.PHEMEX
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.PHEMEX, symbol_type=SymbolTypeEnum.SPOT, native_id='sBTCUSDT'),
        Symbol(exchange_id=ExchangeEnum.PHEMEX, symbol_type=SymbolTypeEnum.PERP_USD, native_id='uBTCUSD'),
        Symbol(exchange_id=ExchangeEnum.PHEMEX, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='BTCUSD'),
    ])

    #############
    # OHLC
    #############
    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#querykline
        endpoint = '/exchange/public/md/kline'
        now = pendulum.now('UTC')
        limit = 1000
        params = {
            'symbol': symbol.native_id,
            'from': now.subtract(minutes=(limit - 1) * timeframe.value).int_timestamp,  # timestamp in s
            'to': now.add(minutes=1).int_timestamp,
            'resolution': timeframe.value * 60,  # resolution in seconds
            'limit': 1000,  # MAX 1000
        }
        return cls._fetch_endpoint(endpoint, params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        labels = ('period', 'interval', 'last_close', 'open', 'high', 'low', 'close')
        return [OHLC.from_list(r[:7], labels) for r in response['data']['rows']]

    @staticmethod
    def _ohlc_fix(parsed_ohlc: list[OHLC], symbol: Symbol, timeframe: TimeFrameEnum) -> list[OHLC]:
        rescale = lambda x: x / (SCALE_FACTOR ** 2 if symbol.symbol_type == SymbolTypeEnum.SPOT else SCALE_FACTOR)
        #scale_fac = SCALE_FACTOR ** 2 if symbol.symbol_type == SymbolTypeEnum.SPOT else SCALE_FACTOR
        for o in parsed_ohlc:
            o.open = rescale(o.open) # / scale_fac
            o.high = rescale(o.high) # / scale_fac
            o.low = rescale(o.low) # / scale_factor
            o.close = rescale(o.close) # / scale_factor
        return parsed_ohlc

    #############
    # Funding
    #############
    @classmethod
    async def _fetch_funding(cls, symbol: Symbol) -> list[PhemexFundingRate]:
        endpoint = '/exchange/public/cfg/fundingRates'
        api_symbol = {
            SymbolTypeEnum.PERP_BTC: '.BTCFR8H',
            SymbolTypeEnum.PERP_USD: '.uBTCFR8H',
        }
        params = {
            'symbol': api_symbol[symbol.symbol_type],
            'offset': 0,
            'limit': 100
        }
        return await cls._fetch_endpoint(endpoint, params=params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[PhemexFundingRate]:
        return parse_obj_as(list[PhemexFundingRate], response['data'])

    ##############
    # Running funding
    ##############
    @classmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        # https://github.com/phemex/phemex-api-docs/blob/master/Public-Contract-API-en.md#query24hrsticker
        endpoint = '/md/ticker/24hr'
        params = {'symbol': symbol.native_id}
        return await cls._fetch_endpoint(endpoint, params=params)

    @staticmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        result = response['result']
        #debug(result)
        return RunningFundingRate(
            timestamp=pendulum.now('UTC'),
            funding_rate=result['fundingRate'] / SCALE_FACTOR ** 2,
            funding_timestamp=pendulum.now('UTC'),  # TODO: fix
            predicted_funding_rate=result['predFundingRate'] / SCALE_FACTOR ** 2
        )
