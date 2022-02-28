from __future__ import annotations

import abc
from typing import NewType, Optional
import datetime as dt

from pydantic import BaseModel, PositiveInt, validator

from src.enums import ExchangeEnum, SymbolTypeEnum, AssetEnum
from src.models.abstract_repository import AbstractRepository


class SymbolError(Exception):
    pass


SymbolNativeId = NewType('SymbolNativeId', str)


class SymbolId(str):
    def __init__(self, symbol_id: str):
        # DERIBIT_PERP:BTC_BTC-PERPETUAL
        try:
            symbol_parts = symbol_id.split('_')
            self._exchange_id = ExchangeEnum(symbol_parts[0])
            self._symbol_type = SymbolTypeEnum(symbol_parts[1])
            self._native_id = SymbolNativeId('_'.join(symbol_parts[2:]))
        except Exception as e:
            raise SymbolError(f'"{symbol_id}" is not a valid SymbolId') from e

    @property
    def exchange_id(self) -> ExchangeEnum:
        return self._exchange_id

    @property
    def symbol_type(self) -> SymbolTypeEnum:
        return self._symbol_type

    @property
    def native_id(self) -> SymbolNativeId:
        return self._native_id

    @property
    def margin(self) -> AssetEnum:
        if AssetEnum.USD in self._symbol_type:
            return AssetEnum.USD
        elif AssetEnum.BTC in self._symbol_type:
            return AssetEnum.BTC
        else:
            raise NotImplementedError

    @classmethod
    def generate(self, exchange_id: ExchangeEnum, symbol_type: SymbolTypeEnum, native_id: SymbolNativeId) -> SymbolId:
        return SymbolId(f'{exchange_id}_{symbol_type}_{native_id}')


class Symbol(BaseModel, abc.ABC, frozen=True):
    id: SymbolId
    ccxt_id: Optional[str] = 'BTC/USD'
    cryptofeed_id: Optional[str]
    lot_size: int = 1  # contract size (in USD) for inverse and minimum order for linear and spot (in SATs)
    order_in_lots: bool = False

    def __init__(self, exchange_id: ExchangeEnum, symbol_type: SymbolTypeEnum, native_id: SymbolNativeId, **kwargs) -> None:
        super().__init__(id=SymbolId.generate(exchange_id, symbol_type, native_id), **kwargs)

    def __hash__(self):
        return hash(self.id)

    @property
    def exchange_id(self) -> ExchangeEnum:
        return self.id.exchange_id

    @property
    def symbol_type(self) -> SymbolTypeEnum:
        return self.id.symbol_type

    @property
    def native_id(self) -> SymbolNativeId:
        return self.id.native_id

    @property
    def margin(self) -> AssetEnum:
        return self.id.margin

    def round_amount(self, amount: int) -> int:
        return round(amount / self.lot_size) * self.lot_size

    def amount_to_lots(self, amount: int) -> int:
        amount = self.round_amount(amount)
        return int(amount / self.lot_size) if self.order_in_lots else amount

    def lots_to_amount(self, lots: int) -> int:
        return lots * self.lot_size if self.order_in_lots else lots



# class FutureSymbol(DerivativeSymbol, abc.ABC):
#     order: PositiveInt
#     maturity: dt.date
#
#     def __init__(self, **kwargs) -> None:
#         kwargs['symbol_type'] = SymbolTypeEnum.FUTURE
#         kwargs['maturity'] = get_nth_maturity_date(kwargs['order'])
#         kwargs['native_id'] = kwargs['ccxt_id'] = self.get_symbol_native_id_from(kwargs['maturity'])
#         super().__init__(**kwargs)
#
#     @staticmethod
#     @abc.abstractmethod
#     def get_symbol_native_id_from(maturity: dt.date) -> str:
#         raise NotImplementedError


class SymbolSet(AbstractRepository):
    """Symbols repository which can be filtered by different attributes"""

    def get_one(self):
        if (n := self.__len__()) != 1:
            raise SymbolError(f'{n} symbols to return instead of 1.')
        return super().get_one()
