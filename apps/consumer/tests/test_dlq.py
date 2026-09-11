"""Dead-letter behaviour for the storage consumer.

The property that matters here is ORDER. The DLQ write must be confirmed
before the source offset is committed -- because produce() only enqueues, and
an unflushed message exists nowhere but in this process's memory. Committing
first reintroduces the offset-watermark bug one layer out: an offset that
acknowledges work which never happened.

Delivery semantics: AT-LEAST-ONCE. A crash between a confirmed DLQ publish and
the offset commit replays the message, and the DLQ has no idempotency key, so
duplicates there are expected rather than exceptional.
"""

from __future__ import annotations

import pytest

import app.consumer

from app.dlq import DeadLetterPublishError
from tests.conftest import (
    FakeDeadLetterProducer,
    FakeSession,
    kafka_consumer_class_logging_to,
    make_event,
    make_malformed_message,
    make_message,
    make_permanent_database_error,
)


OFFSET = 100


def _build_consumer(monkeypatch, save_trade, dlq_fails: bool = False):
    """Wire a consumer whose Kafka commits and DLQ publishes share one log."""
    event_log: list[tuple[str, int]] = []

    monkeypatch.setattr(
        app.consumer, "Consumer", kafka_consumer_class_logging_to(event_log)
    )
    monkeypatch.setattr(app.consumer, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(app.consumer, "save_trade", save_trade)

    # Patch the CLASS, not the instance. TradeStorageConsumer.__init__
    # constructs the producer, so replacing it afterwards still builds a real
    # librdkafka client that tries to reach a broker.
    monkeypatch.setattr(
        app.consumer,
        "DeadLetterProducer",
        lambda **kwargs: FakeDeadLetterProducer(event_log, fail=dlq_fails),
    )

    consumer = app.consumer.TradeStorageConsumer()

    return consumer, event_log


def test_malformed_payload_is_dead_lettered_before_the_offset_is_committed(
    monkeypatch,
):
    """The ordering assertion. Publish confirmed first, commit second."""
    consumer, event_log = _build_consumer(
        monkeypatch, save_trade=lambda session, event: True
    )

    consumer._process_message(make_malformed_message(OFFSET))

    assert event_log == [
        ("dlq_published", OFFSET),
        ("offset_committed", OFFSET),
    ], (
        "The source offset must not advance until the dead-letter write is "
        "confirmed. Committing first acknowledges a message that may exist "
        "only in an in-memory producer queue."
    )


def test_dead_letter_carries_the_original_bytes_not_a_reserialised_event(
    monkeypatch,
):
    """The payload that never parsed is the one the DLQ most needs to carry.

    A DLQ that publishes a serialised TradeEvent cannot represent this case at
    all -- there is no TradeEvent to serialise.
    """
    consumer, _ = _build_consumer(
        monkeypatch, save_trade=lambda session, event: True
    )

    raw = b'{"symbol": "AAPL", "price": '  # truncated mid-object
    consumer._process_message(make_malformed_message(OFFSET, payload=raw))

    published = consumer.dead_letters.published

    assert len(published) == 1
    assert published[0]["value"] == raw, "DLQ value must be the original bytes"
    assert published[0]["key"] == b"AAPL", "original key preserved for partitioning"
    assert published[0]["reason"] == "invalid_payload"


def test_permanent_database_failure_is_dead_lettered_then_committed(monkeypatch):
    """The second loss path. A row PostgreSQL will never accept."""
    poison = make_event(symbol="MSFT")

    def save_trade(session, event):
        if event.event_id == poison.event_id:
            raise make_permanent_database_error()
        return True

    consumer, event_log = _build_consumer(monkeypatch, save_trade=save_trade)

    consumer._process_message(make_message(OFFSET, poison))

    assert event_log == [
        ("dlq_published", OFFSET),
        ("offset_committed", OFFSET),
    ]
    assert consumer.dead_letters.published[0]["reason"] == "database_rejected"
    assert consumer.dead_letters.published[0]["error_type"] == "DataError"


def test_a_failed_dead_letter_write_stops_the_consumer_without_committing(
    monkeypatch,
):
    """If the message cannot be parked, every option except stopping loses it.

    The offset must stay where it is so the message survives in the source
    topic and gets another chance after the restart.
    """
    consumer, event_log = _build_consumer(
        monkeypatch, save_trade=lambda session, event: True, dlq_fails=True
    )

    with pytest.raises(DeadLetterPublishError):
        consumer._process_message(make_malformed_message(OFFSET))

    assert consumer.consumer.committed_offsets == [], (
        "The offset was committed despite the dead-letter write failing. The "
        "message is now unreachable in the source topic and absent from the "
        "DLQ -- silently lost."
    )
    assert event_log == [("dlq_publish_failed", OFFSET)]


def test_healthy_messages_never_touch_the_dead_letter_path(monkeypatch):
    """Guard against a DLQ that quietly swallows successful traffic."""
    consumer, event_log = _build_consumer(
        monkeypatch, save_trade=lambda session, event: True
    )

    for offset in (OFFSET, OFFSET + 1, OFFSET + 2):
        consumer._process_message(make_message(offset, make_event()))

    assert consumer.dead_letters.published == []
    assert consumer.consumer.committed_offsets == [OFFSET, OFFSET + 1, OFFSET + 2]