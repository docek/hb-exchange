from src.enums import ExchangeEnum, SymbolTypeEnum, TimeFrameEnum
from src.exchanges import Binance, Bitfinex, Bitstamp, Bitmex, Bybit, Coinbase, Deribit, Ftx, Huobi, Kraken, Okex, \
    Phemex
from src.models import OHLC, FundingRate

CLIENTS = {
    ExchangeEnum.BINANCE: Binance(),
    ExchangeEnum.BITFINEX: Bitfinex(),
    ExchangeEnum.BITSTAMP: Bitstamp(),
    ExchangeEnum.BITMEX: Bitmex(),
    ExchangeEnum.BYBIT: Bybit(),
    ExchangeEnum.COINBASE: Coinbase(),
    ExchangeEnum.DERIBIT: Deribit(),
    ExchangeEnum.FTX: Ftx(),
    ExchangeEnum.HUOBI: Huobi(),
    ExchangeEnum.KRAKEN: Kraken(),
    ExchangeEnum.OKEX: Okex(),
    ExchangeEnum.PHEMEX: Phemex(),
}


async def get_ohlc(exchange_id: ExchangeEnum, symbol_type: SymbolTypeEnum, timeframe: TimeFrameEnum, **kwargs) -> list[
    OHLC]:
    return await CLIENTS[exchange_id].get_ohlc(symbol_type, timeframe, **kwargs)


async def get_funding(exchange_id: ExchangeEnum, symbol_type: SymbolTypeEnum, **kwargs) -> list[FundingRate]:
    return await CLIENTS[exchange_id].get_funding(symbol_type, **kwargs)


async def get_running_funding(exchange_id: ExchangeEnum, symbol_type: SymbolTypeEnum) -> FundingRate:
    return await CLIENTS[exchange_id].get_running_funding(symbol_type)
