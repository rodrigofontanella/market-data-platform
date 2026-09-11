"""Test doubles for the storage consumer.

Level 2a in the testing pyramid: real consumer logic, fake Kafka, fake database.
Nothing here touches a broker, a socket or PostgreSQL.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import DataError, OperationalError

from market_core import TradeEvent


TOPIC = "market.trades.raw"
PARTITION = 0


# ---------------------------------------------------------------------------
# Fake Kafka
# ---------------------------------------------------------------------------


class FakeKafkaConsumer:
    """Stands in for confluent_kafka.Consumer.

    Records every offset it is asked to commit, in the order it was asked.
    That recording is the entire test surface: the questions we care about
    are "was the offset advanced?" and "how far?", not "did Kafka work?".
    """

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self.committed_offsets: list[int] = []
        self.subscribed_topics: list[str] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed_topics = list(topics)

    def commit(self, message=None, asynchronous: bool = True) -> None:
        if message is None:
            raise AssertionError(
                "commit() was called without a message. The consumer is "
                "supposed to commit explicitly, per message."
            )
        self.committed_offsets.append(message.offset())

    def close(self) -> None:
        self.closed = True


class FakeMessage:
    """Stands in for confluent_kafka.Message.

    confluent_kafka exposes these as methods, not attributes -- msg.offset(),
    not msg.offset -- so the fake has to do the same or the code under test
    will silently compare a bound method to an int.
    """

    def __init__(
        self,
        offset: int,
        payload: bytes,
        topic: str = TOPIC,
        partition: int = PARTITION,
    ) -> None:
        self._offset = offset
        self._payload = payload
        self._topic = topic
        self._partition = partition

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._payload

    def error(self):
        return None


# ---------------------------------------------------------------------------
# Fake database
# ---------------------------------------------------------------------------


class FakeSession:
    """Context-manager stand-in for a SQLAlchemy Session.

    The consumer uses it as `with SessionLocal() as session:` and then calls
    commit() or rollback(), so those are the only three behaviours needed.
    """

    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False

    def __enter__(self) -> FakeSession:
        return self

    def __exit__(self, *exc_info) -> bool:
        return False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def make_transient_database_error(
    message: str = "connection refused",
) -> OperationalError:
    """A database that went away. The message is fine; retrying will work.

    SQLAlchemy's DBAPIError subclasses take (statement, params, orig).
    OperationalError is what you actually get when PostgreSQL stops --
    the failure we reproduced by stopping the postgres container.
    """
    return OperationalError("INSERT INTO trades ...", {}, Exception(message))


def make_permanent_database_error(
    message: str = "numeric field overflow",
) -> DataError:
    """A row the database will never accept, however many times we try.

    DataError is the realistic case here: a price that does not fit
    NUMERIC(18,6). Retrying blocks the partition forever, so this class of
    failure must be skipped rather than retried.
    """
    return DataError("INSERT INTO trades ...", {}, Exception(message))


# ---------------------------------------------------------------------------
# Deterministic event / message builders
# ---------------------------------------------------------------------------


FIXED_TIMESTAMP = datetime(2026, 9, 7, 12, 0, 0, tzinfo=UTC)


def make_event(
    symbol: str = "AAPL",
    price: str = "215.00",
    volume: int = 100,
    event_id: UUID | None = None,
) -> TradeEvent:
    """A valid TradeEvent with no randomness and no wall clock.

    Both would make assertions non-deterministic; a test that passes 99% of
    the time trains you to ignore failures.
    """
    return TradeEvent(
        event_id=event_id or uuid4(),
        symbol=symbol,
        price=Decimal(price),
        volume=volume,
        timestamp=FIXED_TIMESTAMP,
        source="test",
    )


def make_message(offset: int, event: TradeEvent) -> FakeMessage:
    return FakeMessage(offset=offset, payload=event.model_dump_json().encode())


@pytest.fixture
def fake_kafka_consumer_class():
    return FakeKafkaConsumer


@pytest.fixture
def fake_session_factory():
    """Patched over app.consumer.SessionLocal; returns a fresh FakeSession."""
    return lambda: FakeSession()
