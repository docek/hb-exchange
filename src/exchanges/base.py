from __future__ import annotations

import abc
from contextlib import asynccontextmanager
from typing import ClassVar, Any, AsyncGenerator
import datetime as dt

import httpx
import pendulum
from devtools import debug
from loguru import logger

from src.enums import TimeFrameEnum, ExchangeEnum, SymbolTypeEnum
from src.models import OHLC, SymbolSet, Symbol, FundingRate
from src.models.funding_rate import RunningFundingRate


class AbstractBaseExchange(abc.ABC):

    @classmethod
    async def get_ohlc(cls, symbol_type: SymbolTypeEnum, timeframe: TimeFrameEnum) -> list[OHLC]:
        raise NotImplementedError


class BaseExchange:
    API_BASE_PATH: ClassVar[str]
    EXCHANGE_ID: ClassVar[ExchangeEnum]
    SYMBOLS: ClassVar[SymbolSet]

    @classmethod
    def _get_symbol(cls, symbol_type: SymbolTypeEnum) -> Symbol:
        return cls.SYMBOLS.find(symbol_type=symbol_type).get_one()

    @classmethod
    @asynccontextmanager
    async def _get_client(cls) -> AsyncGenerator:
        client = httpx.AsyncClient(base_url=cls.API_BASE_PATH)
        try:
            yield client
        finally:
            await client.aclose()

    @classmethod
    async def _fetch_endpoint(cls, endpoint: str, params: dict[str, str | int] = None) -> Any:
        async with cls._get_client() as client:
            response = await client.get(endpoint, follow_redirects=True, params=params)
            logger.debug(response.url)
            if response.status_code == httpx.codes.OK:
                return response.json()
            else:
                logger.warning(f'{cls.__name__} {response.status_code}{response.json()}')
                return None

    #################
    # OHLC
    #################
    @staticmethod
    def _ohlc_fix(parsed_ohlc: list[OHLC], symbol: Symbol, timeframe: TimeFrameEnum) -> list[OHLC]:
        return parsed_ohlc

    @staticmethod
    @abc.abstractmethod
    def _parse_ohlc(response: dict[str, Any]) -> list[OHLC]:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    async def _fetch_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum):
        raise NotImplementedError

    @classmethod
    @logger.catch
    async def _get_ohlc(cls, symbol: Symbol, timeframe: TimeFrameEnum) -> list[OHLC]:
        parsed_ohlc = cls._parse_ohlc(await cls._fetch_ohlc(symbol, timeframe))
        return cls._ohlc_fix(parsed_ohlc, symbol, timeframe)

    @classmethod
    async def get_ohlc(cls, symbol_type: SymbolTypeEnum, timeframe: TimeFrameEnum, since: dt.datetime = None,
                       include_unfinished: bool = False):
        fetched_ohlc = sorted(await cls._get_ohlc(cls._get_symbol(symbol_type), timeframe), key=lambda x: x.period)
        since = since if since else fetched_ohlc[0].period - dt.timedelta(minutes=1)
        until = pendulum.now('UTC').subtract(minutes=0 if include_unfinished else timeframe.value)
        return [ohlc for ohlc in fetched_ohlc if since < ohlc.period < until]

    ##################
    # Funding
    ##################
    @staticmethod
    @abc.abstractmethod
    def _parse_funding(response: dict[str, Any]) -> list[FundingRate]:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    async def _fetch_funding(cls, symbol: Symbol):
        raise NotImplementedError


    @classmethod
    @logger.catch(default=[])
    async def get_funding(cls, symbol_type: SymbolTypeEnum, since: dt.datetime = None) -> list[FundingRate]:
        symbol = cls._get_symbol(symbol_type)
        fetched_funding = sorted(cls._parse_funding(await cls._fetch_funding(symbol)), key=lambda x: x.timestamp)
        since = since if since else fetched_funding[0].timestamp - dt.timedelta(minutes=1)
        return [funding for funding in fetched_funding if funding.timestamp > since]

    ##################
    # Running funding
    ##################
    @staticmethod
    @abc.abstractmethod
    def _parse_running_funding(response: dict[str, Any]) -> RunningFundingRate:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    async def _fetch_running_funding(cls, symbol: Symbol):
        raise NotImplementedError

    @classmethod
    @logger.catch(default=None)
    async def get_running_funding(cls, symbol_type: SymbolTypeEnum) -> RunningFundingRate:
        symbol = cls._get_symbol(symbol_type)
        fetched_running_funding_rate = await cls._fetch_running_funding(symbol)
        running_funding_rate = cls._parse_running_funding(fetched_running_funding_rate)
        now = pendulum.now('UTC')
        if abs((running_funding_rate.timestamp - now).seconds)> 60:
            raise ValueError(f'Running funding rate {running_funding_rate} not up to date for {now}.')
        return running_funding_rate