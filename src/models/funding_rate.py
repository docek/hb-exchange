import datetime as dt
from typing import Optional

import pendulum
from pydantic import BaseModel, Field


class FundingRate(BaseModel):
    timestamp: dt.datetime
    funding_rate: float

class RunningFundingRate(BaseModel):
    timestamp: dt.datetime
    funding_timestamp: dt.datetime
    funding_rate: float
    predicted_funding_rate: Optional[float]