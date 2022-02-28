import datetime as dt
from typing import Any, ClassVar

import pendulum
from devtools import debug
from loguru import logger
from pydantic import Field, parse_obj_as

from src.enums import ExchangeEnum, SymbolTypeEnum, TimeFrameEnum
from src.exchanges.base import BaseExchange, AbstractBaseExchange
from src.models import FundingRate, OHLC, SymbolSet, Symbol
from src.models.funding_rate import RunningFundingRate

TIMEFRAME = {
    TimeFrameEnum.MINUTE: '1min',
    TimeFrameEnum.HOUR: '1hour',
    TimeFrameEnum.DAY: '1day',
}


class HuobiFundingRate(FundingRate):
    timestamp: dt.datetime = Field(alias='funding_time')
    funding_rate: float = Field(alias='realized_rate')


class HuobiOhlc(OHLC):
    period: dt.datetime = Field(alias='id')


class HuobiBaseExchange(BaseExchange):
    API_BASE_PATH = 'https://api.huobi.pro'
    EXCHANGE_ID = ExchangeEnum.HUOBI
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.HUOBI, symbol_type=SymbolTypeEnum.SPOT, native_id='btcusdt'),
        Symbol(exchange_id=ExchangeEnum.HUOBI, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='btc-usd'),
        Symbol(exchange_id=ExchangeEnum.HUOBI, symbol_type=SymbolTypeEnum.PERP_USD, native_id='btc-usdt'),
    ])

    #############
    # OHLC
    #############
    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://huobiapi.github.io/docs/spot/v1/en/#get-klines-candles
        # https://huobiapi.github.io/docs/coin_margined_swap/v1/en/#get-kline-data
        # https://huobiapi.github.io/docs/usdt_swap/v1/en/#general-get-kline-data
        endpoint = {
            SymbolTypeEnum.SPOT: '/market/history/kline',
            SymbolTypeEnum.PERP_BTC: '/swap-ex/market/history/kline',
            SymbolTypeEnum.PERP_USD: '/linear-swap-ex/market/history/kline',
        }
        params = {
            'symbol': symbol.native_id,
            'contract_code': symbol.native_id,
            'period': TIMEFRAME[timeframe],
            'size': 2000,  # max 2000, default 150
        }
        return cls._fetch_endpoint(endpoint[symbol.symbol_type], params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        parsed_ohlc = parse_obj_as(list[HuobiOhlc], response['data'])
        for ohlc in parsed_ohlc:
            ohlc.period = ohlc.period + dt.timedelta(hours=8)
        return parsed_ohlc

    #############
    # Funding
    #############
    @classmethod
    async def _fetch_funding(cls, symbol: Symbol) -> list[FundingRate]:
        # https://huobiapi.github.io/docs/coin_margined_swap/v1/en/#query-historical-funding-rate
        # https://huobiapi.github.io/docs/usdt_swap/v1/en/#general-query-historical-funding-rate
        endpoint = {
            SymbolTypeEnum.PERP_BTC: 'swap-api/v1/swap_historical_funding_rate',
            SymbolTypeEnum.PERP_USD: '/linear-swap-api/v1/swap_historical_funding_rate',
        }
        params = {
            'contract_code': symbol.native_id,
            # 'page_index': 1,
            # 'page_size': 50
        }
        return await cls._fetch_endpoint(endpoint[symbol.symbol_type], params=params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[FundingRate]:
        return parse_obj_as(list[HuobiFundingRate], response['data']['data'])

    ##############
    # Running funding
    ##############
    @classmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        # https://huobiapi.github.io/docs/coin_margined_swap/v1/en/#query-funding-rate
        # https://huobiapi.github.io/docs/usdt_swap/v1/en/#general-query-funding-rate
        endpoint = {
            SymbolTypeEnum.PERP_BTC: '/swap-api/v1/swap_funding_rate',
            SymbolTypeEnum.PERP_USD: '/linear-swap-api/v1/swap_funding_rate',
        }
        params = {
            'contract_code': symbol.native_id,
        }
        return await cls._fetch_endpoint(endpoint[symbol.symbol_type], params=params)

    @staticmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        result = response['data']
        # debug(result)
        return RunningFundingRate(
            timestamp=pendulum.now('UTC'),
            funding_timestamp=result['funding_time'],
            funding_rate=result['funding_rate'],
            predicted_funding_rate=result['estimated_rate']
        )


class HuobiSpotExchange(HuobiBaseExchange):
    API_BASE_PATH = 'https://api.huobi.pro'


class HuobiPerpExchange(HuobiBaseExchange):
    API_BASE_PATH = 'https://api.hbdm.com'


class Exchange(AbstractBaseExchange):
    EXCHANGE_ID: ExchangeEnum = ExchangeEnum.HUOBI
    SPOT: ClassVar[HuobiSpotExchange] = HuobiSpotExchange()
    PERP: ClassVar[HuobiPerpExchange] = HuobiPerpExchange()

    @classmethod
    async def get_ohlc(cls, symbol_type: SymbolTypeEnum, timeframe: TimeFrameEnum) -> list[OHLC]:
        match symbol_type:
            case SymbolTypeEnum.SPOT:
                return await cls.SPOT.get_ohlc(symbol_type, timeframe)
            case SymbolTypeEnum.PERP_BTC | SymbolTypeEnum.PERP_USD:
                return await cls.PERP.get_ohlc(symbol_type, timeframe)

    @classmethod
    async def get_funding(cls, symbol_type: SymbolTypeEnum, **kwargs) -> list[FundingRate]:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC | SymbolTypeEnum.PERP_USD:
                return await cls.PERP.get_funding(symbol_type, **kwargs)
            case _:
                raise NotImplementedError

    @classmethod
    async def get_running_funding(cls, symbol_type: SymbolTypeEnum) -> RunningFundingRate:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC | SymbolTypeEnum.PERP_USD:
                return await cls.PERP.get_running_funding(symbol_type)
            case _:
                raise NotImplementedError
