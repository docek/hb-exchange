from typing import NewType

from pydantic import NonNegativeFloat

PriceQuote = NewType('PriceQuote', NonNegativeFloat)