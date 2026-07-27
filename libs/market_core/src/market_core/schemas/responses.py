from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class TradeResponse(BaseModel):
    """Public trade representation returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: UUID
    event_type: str
    symbol: str
    price: Decimal
    volume: int
    event_time: datetime
    source: str
    schema_version: int
    created_at: datetime
