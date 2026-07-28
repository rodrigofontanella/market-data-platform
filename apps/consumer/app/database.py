import logging
from collections.abc import Generator
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from market_core import Trade, TradeEvent

logger = logging.getLogger(__name__)

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()

    try:
        yield session
    finally:
        session.close()


def save_trade(
    session: Session,
    event: TradeEvent,
) -> bool:
    statement = (
        insert(Trade)
        .values(
            event_id=event.event_id,
            event_type=event.event_type,
            symbol=event.symbol,
            price=event.price,
            volume=event.volume,
            event_time=event.timestamp,
            source=event.source,
            schema_version=event.schema_version,
        )
        .on_conflict_do_nothing(
            index_elements=[Trade.event_id],
        )
        .returning(Trade.id)
    )

    inserted_id = session.execute(statement).scalar_one_or_none()

    return inserted_id is not None