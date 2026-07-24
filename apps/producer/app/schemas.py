from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class TradeEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str = "trade"
    symbol: str
    price: float = Field(gt=0)
    volume: int = Field(gt=0)
    timestamp: datetime
    source: str = "simulator"
    schema_version: int = 1