from contextlib import asynccontextmanager
from typing import AsyncGenerator

import httpx
import pendulum
from devtools import debug
from loguru import logger

from config import SETUP
from src.enums import SymbolTypeEnum

API_BASE_URL = 'https://open-api.coinglass.com/api/pro/v1/'
HEADER = {'coinglassSecret': SETUP.coinglass_api_key.get_secret_value()}


@asynccontextmanager
async def _get_client() -> AsyncGenerator:
    client = httpx.AsyncClient(base_url=API_BASE_URL, headers=HEADER)
    try:
        yield client
    finally:
        await client.aclose()


async def _fetch_funding_rate(symbol_type: SymbolTypeEnum):
    # returns the predicted funding rates
    # https://coinglass.github.io/API-Reference/#funding-rates-chart
    async with _get_client() as client:
        url = 'futures/funding_rates_chart?symbol=BTC&type='
        match symbol_type:
            case SymbolTypeEnum.PERP_BTC:
                url += 'C'
            case SymbolTypeEnum.PERP_USD:
                url += 'U'
        response = await client.get(url)
        if response.status_code == httpx.codes.OK:
            return response.json()
        else:
            logger.warning(f'{response.status_code}{response.json()}')
            return None


def _parse_funding_rate(response):
    #debug(response)
    data_time = pendulum.from_timestamp(response['data']['dateList'][-1]/1000)
    now = pendulum.now('UTC')
    if abs((data_time - now).seconds) > 10:
        raise ValueError(f'Data time {data_time} to different form now {now}')
    debug(response['data']['dataMap'])
    return {exchange: rates[-1] for exchange, rates in response['data']['dataMap'].items()}


async def get_estimated_funding_rate(symbol_type: SymbolTypeEnum):
    fetched_response = await _fetch_funding_rate(symbol_type)
    return _parse_funding_rate(fetched_response)
