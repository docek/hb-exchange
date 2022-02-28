
import datetime as dt
import functools
import inspect
from collections import defaultdict
from statistics import median

from loguru import logger

from src.models import OHLC

def btc_to_sat(amount: float) -> int:
    return int(amount*100_000_000)

def sat_to_btc(amount: int) -> float:
    return amount/100_000_000

def sort_ohlc(ohlc: list[OHLC]) -> list[OHLC]:
    """Returns OHLC list sorted by period."""
    return sorted(ohlc, key=lambda x: x.period)


def sort_ohlc_to_periods(ohlc: list[OHLC]) -> dict[dt.datetime, list[OHLC]]:
    """Returns a list of OHLC per period"""
    periods = defaultdict(list)
    for o in ohlc:
        periods[o.period].append(o)
    return periods


def get_median_ohlc(ohlc: list[OHLC]) -> list[OHLC]:
    """Returns OHLC list sorted by period. Multiple OHLC from same period on input transformed into median OHLC on output."""

    def median_for_period(ohlc: list[OHLC]) -> OHLC:
        """Returns median for given OHLC list"""
        return OHLC(
            period=ohlc[0].period,  # expects all ohlc from save period
            open=median([o.open for o in ohlc]),
            high=median([o.high for o in ohlc]),
            low=median([o.low for o in ohlc]),
            close=median([o.close for o in ohlc]),
        )

    return sort_ohlc([median_for_period(period) for period in sort_ohlc_to_periods(ohlc).values()])



def debug_func(decorated_function):
    """
    Function decorator logging entry + exit and parameters and result of functions.

    """

    @functools.wraps(decorated_function)
    def wrapper(*dec_fn_args, **dec_fn_kwargs):
        # Log function entry
        func_name = decorated_function.__name__
        name_dict = dict(func_name=func_name)
        logger.debug(f"Entering {func_name}()...", extra=name_dict)

        # Log function params (args and kwargs)
        func_args = inspect.signature(decorated_function).bind(*dec_fn_args, **dec_fn_kwargs).arguments
        func_args_str = ', '.join(
            f"{var_name} = {var_value}"
            for var_name, var_value
            in func_args.items()
        )
        logger.debug(f"\t{func_args_str}", extra=name_dict)

        # Execute wrapped (decorated) function:
        out = decorated_function(*dec_fn_args, **dec_fn_kwargs)

        logger.debug(f'Returning: ', out)
        logger.debug(f"Done running {func_name}()!", extra=name_dict)

        return out
    return wrapper
