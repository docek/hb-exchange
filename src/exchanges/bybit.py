from typing import ClassVar

from src.exchanges.base import AbstractBaseExchange
from .bybit_perp import Exchange as BybitPerp
from .bybit_spot import Exchange as BybitSpot
from ..enums import SymbolTypeEnum, TimeFrameEnum, ExchangeEnum
from ..models import OHLC, FundingRate


class Exchange(AbstractBaseExchange):
    EXCHANGE_ID: ExchangeEnum = ExchangeEnum.BYBIT
    PERP: ClassVar[BybitPerp] = BybitPerp()
    SPOT: ClassVar[BybitSpot] = BybitSpot()

    @classmethod
    async def get_ohlc(cls, symbol_type: SymbolTypeEnum, timeframe: TimeFrameEnum) -> list[OHLC]:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC | SymbolTypeEnum.PERP_USD:
                return await cls.PERP.get_ohlc(symbol_type, timeframe)
            case SymbolTypeEnum.SPOT:
                return await cls.SPOT.get_ohlc(symbol_type, timeframe)

    @classmethod
    async def get_funding(cls, symbol_type: SymbolTypeEnum, **kwargs) -> list[FundingRate]:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC | SymbolTypeEnum.PERP_USD:
                return await cls.PERP.get_funding(symbol_type, **kwargs)
            case _:
                raise NotImplementedError

    @classmethod
    async def get_running_funding(cls, symbol_type: SymbolTypeEnum) -> FundingRate:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC | SymbolTypeEnum.PERP_USD:
                return await cls.PERP.get_running_funding(symbol_type)
            case _:
                raise NotImplementedError