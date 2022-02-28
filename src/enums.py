from enum import Enum


###################
# Assets
###################

class AssetEnum(str, Enum):
    BTC = 'BTC'
    USD = 'USD'

###################
# Exchanges
###################

class ExchangeEnum(str, Enum):
    DERIBIT = 'DERI'
    BITMEX = 'BITM'
    BYBIT = 'BYBI'
    PHEMEX = 'PHEM'
    BINANCE = 'BINA'
    FTX = 'FTXX'
    KRAKEN = 'KRAK'
    OKEX = 'OKEX'
    HUOBI = 'HUOB'
    BITSTAMP = 'BITS'
    BITFINEX = 'BITF'
    COINBASE = 'COIN'


class TimeFrameEnum(int, Enum):
    MINUTE = 1
    HOUR = 60
    DAY = 60*24

class ProductEnum(str, Enum):
    OHLC = 'OHLC'
    FUNDING = 'FUNDING'


class SymbolTypeEnum(str, Enum):
    SPOT = 'SPOT'
    PERP_BTC = 'PERP:BTC'
    PERP_USD = 'PERP:USD'
