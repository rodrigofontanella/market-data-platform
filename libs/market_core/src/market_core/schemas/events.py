from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class TradeEvent(BaseModel):
    """Canonical trade event published to Kafka."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    event_type: Literal["trade"] = "trade"

    symbol: str = Field(
        min_length=1,
        max_length=20,
    )
    price: Decimal = Field(gt=0)
    volume: int = Field(gt=0)
    timestamp: datetime

    source: str = Field(
        min_length=1,
        max_length=100,
    )
    schema_version: int = Field(
        default=1,
        ge=1,
    )
