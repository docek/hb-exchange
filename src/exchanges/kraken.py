from typing import ClassVar

from src.exchanges.base import AbstractBaseExchange
from .kraken_perp import Exchange as KrakenPerp
from .kraken_spot import Exchange as KrakenSpot
from ..enums import SymbolTypeEnum, ExchangeEnum
from ..models import OHLC, FundingRate
from ..models.funding_rate import RunningFundingRate


class Exchange(AbstractBaseExchange):
    EXCHANGE_ID: ExchangeEnum = ExchangeEnum.KRAKEN
    SPOT: ClassVar[KrakenSpot] = KrakenSpot()
    PERP: ClassVar[KrakenPerp] = KrakenPerp()

    @classmethod
    async def get_ohlc(cls, symbol_type: SymbolTypeEnum, *args, **kwargs) -> list[OHLC]:
        match symbol_type:
            case SymbolTypeEnum.SPOT:
                return await cls.SPOT.get_ohlc(symbol_type, *args, **kwargs)
            case SymbolTypeEnum.PERP_BTC:
                return await cls.PERP.get_ohlc(symbol_type, *args, **kwargs)

    @classmethod
    async def get_funding(cls, symbol_type: SymbolTypeEnum, **kwargs) -> list[FundingRate]:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC:
                return await cls.PERP.get_funding(symbol_type, **kwargs)
            case _:
                raise NotImplementedError

    @classmethod
    async def get_running_funding(cls, symbol_type: SymbolTypeEnum) -> RunningFundingRate:
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC:
                return await cls.PERP.get_running_funding(symbol_type)
            case _:
                raise NotImplementedError