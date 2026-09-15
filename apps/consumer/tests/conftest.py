"""Test doubles for the storage consumer.

Level 2a in the testing pyramid: real consumer logic, fake Kafka, fake database.
Nothing here touches a broker, a socket or PostgreSQL.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from app.dlq import DeadLetterPublishError
from market_core import TradeEvent
from sqlalchemy.exc import DataError, OperationalError

TOPIC = "market.trades.raw"
PARTITION = 0


# ---------------------------------------------------------------------------
# Fake Kafka
# ---------------------------------------------------------------------------


class FakeKafkaConsumer:
    """Stands in for confluent_kafka.Consumer.

    Records every offset it is asked to commit, in the order it was asked.
    """

    event_log: list | None = None

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

        if self.event_log is not None:
            self.event_log.append(("offset_committed", message.offset()))

    def close(self) -> None:
        self.closed = True


def kafka_consumer_class_logging_to(event_log: list) -> type[FakeKafkaConsumer]:
    """A FakeKafkaConsumer subclass that appends commits to a shared log.

    Ordering between two collaborators cannot be asserted from two separate
    lists -- they have to write to the same one.
    """
    return type("LoggingFakeKafkaConsumer", (FakeKafkaConsumer,), {"event_log": event_log})


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
        key: bytes | None = None,
        topic: str = TOPIC,
        partition: int = PARTITION,
    ) -> None:
        self._offset = offset
        self._payload = payload
        self._key = key
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

    def key(self) -> bytes | None:
        return self._key

    def error(self):
        return None


# ---------------------------------------------------------------------------
# Fake database
# ---------------------------------------------------------------------------


class FakeSession:
    """Context-manager stand-in for a SQLAlchemy Session."""

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
    """A database that went away. The message is fine; retrying will work."""
    return OperationalError("INSERT INTO trades ...", {}, Exception(message))


def make_permanent_database_error(
    message: str = "numeric field overflow",
) -> DataError:
    """A row the database will never accept, however many times we try."""
    return DataError("INSERT INTO trades ...", {}, Exception(message))


# ---------------------------------------------------------------------------
# Fake dead-letter producer
# ---------------------------------------------------------------------------


class FakeDeadLetterProducer:
    """Stands in for app.dlq.DeadLetterProducer.

    Records publishes in a shared event log alongside the consumer's offset
    commits, because the property under test is ORDER: the DLQ write must be
    confirmed before the source offset moves.
    """

    def __init__(self, event_log: list, fail: bool = False) -> None:
        self.event_log = event_log
        self.fail = fail
        self.published: list[dict] = []
        self.closed = False

    def publish(self, message, error, reason, consumer_group) -> None:
        if self.fail:
            self.event_log.append(("dlq_publish_failed", message.offset()))
            raise DeadLetterPublishError("broker unreachable")

        self.published.append(
            {
                "offset": message.offset(),
                "value": message.value(),
                "key": message.key(),
                "reason": reason,
                "error_type": type(error).__name__,
                "consumer_group": consumer_group,
            }
        )
        self.event_log.append(("dlq_published", message.offset()))

    def close(self) -> None:
        self.closed = True


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
    """A valid TradeEvent with no randomness and no wall clock."""
    return TradeEvent(
        event_id=event_id or uuid4(),
        symbol=symbol,
        price=Decimal(price),
        volume=volume,
        timestamp=FIXED_TIMESTAMP,
        source="test",
    )


def make_message(offset: int, event: TradeEvent) -> FakeMessage:
    return FakeMessage(
        offset=offset,
        payload=event.model_dump_json().encode(),
        key=event.symbol.encode(),
    )


def make_malformed_message(
    offset: int,
    payload: bytes = b"{not json at all",
    key: bytes | None = b"AAPL",
) -> FakeMessage:
    """A payload that never parses -- the case a re-serialised DLQ cannot carry."""
    return FakeMessage(offset=offset, payload=payload, key=key)


@pytest.fixture
def fake_session_factory():
    """Patched over app.consumer.SessionLocal; returns a fresh FakeSession."""
    return lambda: FakeSession()