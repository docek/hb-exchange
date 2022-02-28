import datetime as dt
from typing import Any, ClassVar

import pendulum
from pydantic import Field, validator, parse_obj_as

from src.enums import ExchangeEnum, SymbolTypeEnum, TimeFrameEnum
from src.exchanges.base import BaseExchange, AbstractBaseExchange
from src.models import OHLC, SymbolSet, Symbol, FundingRate
from src.models.funding_rate import RunningFundingRate

TIMEFRAME = {
    TimeFrameEnum.MINUTE: '1m',
    TimeFrameEnum.HOUR: '1h',
    TimeFrameEnum.DAY: '1d',
}


class BinanceFundingRate(FundingRate):
    timestamp: dt.datetime = Field(alias='fundingTime')
    funding_rate: float = Field(alias='fundingRate')

    @validator('timestamp')
    def round_to_hours(cls, v) -> dt.datetime:
        return v.replace(minute=0, second=0, microsecond=0)


class BinanceBaseExchange(BaseExchange):
    API_BASE_PATH = ''
    EXCHANGE_ID = ExchangeEnum.BINANCE
    SYMBOLS = SymbolSet([
        Symbol(exchange_id=ExchangeEnum.BINANCE, symbol_type=SymbolTypeEnum.SPOT, native_id='BTCUSDT'),
        Symbol(exchange_id=ExchangeEnum.BINANCE, symbol_type=SymbolTypeEnum.PERP_BTC, native_id='BTCUSD_PERP'),
        Symbol(exchange_id=ExchangeEnum.BINANCE, symbol_type=SymbolTypeEnum.PERP_USD, native_id='BTCUSDT'),
    ])

    @classmethod
    def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        # https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        # https://binance-docs.github.io/apidocs/delivery/en/#kline-candlestick-data
        # https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
        endpoint = {
            SymbolTypeEnum.SPOT: '/api/v3/klines',
            SymbolTypeEnum.PERP_BTC: '/dapi/v1/klines',
            SymbolTypeEnum.PERP_USD: '/fapi/v1/klines',
        }
        params = {
            'symbol': symbol.native_id,
            'interval': TIMEFRAME[timeframe],
        }
        return cls._fetch_endpoint(endpoint[symbol.symbol_type], params)

    @staticmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        return [OHLC.from_list(r[:5]) for r in response]

    @classmethod
    async def _fetch_funding(cls, symbol: Symbol) -> list[FundingRate]:
        # https://binance-docs.github.io/apidocs/delivery/en/#get-funding-rate-history-of-perpetual-futures
        # https://binance-docs.github.io/apidocs/futures/en/#get-funding-rate-history
        endpoint = {
            SymbolTypeEnum.PERP_BTC: '/dapi/v1/fundingRate',
            SymbolTypeEnum.PERP_USD: '/fapi/v1/fundingRate',
        }
        limit = 1000
        params = {
            'symbol': symbol.native_id,
            'startTime': pendulum.now('UTC').subtract(days=int(limit / 3)).int_timestamp * 1000,
            'limit': limit
        }
        return await cls._fetch_endpoint(endpoint[symbol.symbol_type], params=params)

    @staticmethod
    def _parse_funding(response: dict[str, Any]) -> list[BinanceFundingRate]:
        return parse_obj_as(list[BinanceFundingRate], response)

    #################
    # Running funding
    #################
    @classmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        # https://binance-docs.github.io/apidocs/delivery/en/#index-price-and-mark-price
        # https://binance-docs.github.io/apidocs/futures/en/#mark-price
        endpoint = {
            SymbolTypeEnum.PERP_BTC: '/dapi/v1/premiumIndex',
            SymbolTypeEnum.PERP_USD: '/fapi/v1/premiumIndex',
        }
        params = {'symbol': symbol.native_id}
        return await cls._fetch_endpoint(endpoint[symbol.symbol_type], params=params)

    @staticmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        # for Coin-M list is returned
        result = response if not isinstance(response, list) else response.pop()
        # debug(result)
        return RunningFundingRate(
            timestamp=result['time'],
            funding_timestamp=result['nextFundingTime'],
            funding_rate=result['lastFundingRate']
        )


class BinanceSpotExchange(BinanceBaseExchange):
    API_BASE_PATH = 'https://api.binance.com'


class BinanceDeliveryExchange(BinanceBaseExchange):
    API_BASE_PATH = 'https://dapi.binance.com'


class BinanceFutureExchange(BinanceBaseExchange):
    API_BASE_PATH = 'https://fapi.binance.com'


class Exchange(AbstractBaseExchange):
    EXCHANGE_ID: ExchangeEnum = ExchangeEnum.BINANCE
    SPOT: ClassVar[BinanceSpotExchange] = BinanceSpotExchange()
    PERP_BTC: ClassVar[BinanceDeliveryExchange] = BinanceDeliveryExchange()
    PERP_USD: ClassVar[BinanceFutureExchange] = BinanceFutureExchange()

    @classmethod
    async def get_ohlc(cls, symbol_type: SymbolTypeEnum, timeframe: TimeFrameEnum, **kwargs) -> list[OHLC]:
        match symbol_type:
            case SymbolTypeEnum.SPOT:
                return await cls.SPOT.get_ohlc(symbol_type, timeframe, **kwargs)
            case SymbolTypeEnum.PERP_BTC:
                return await cls.PERP_BTC.get_ohlc(symbol_type, timeframe, **kwargs)
            case SymbolTypeEnum.PERP_USD:
                return await cls.PERP_USD.get_ohlc(symbol_type, timeframe, **kwargs)

    @classmethod
    async def get_funding(cls, symbol_type: SymbolTypeEnum, **kwargs) -> list[FundingRate]:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC:
                return await cls.PERP_BTC.get_funding(symbol_type, **kwargs)
            case SymbolTypeEnum.PERP_USD:
                return await cls.PERP_USD.get_funding(symbol_type, **kwargs)
            case _:
                raise NotImplementedError

    @classmethod
    async def get_running_funding(cls, symbol_type: SymbolTypeEnum) -> RunningFundingRate:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC:
                return await cls.PERP_BTC.get_running_funding(symbol_type)
            case SymbolTypeEnum.PERP_USD:
                return await cls.PERP_USD.get_running_funding(symbol_type)
            case _:
                raise NotImplementedError
