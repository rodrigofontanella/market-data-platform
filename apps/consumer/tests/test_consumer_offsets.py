"""Offset-commit semantics for the storage consumer.

A committed offset is a partition-level watermark, not a per-message
acknowledgement. Kafka stores exactly one number per (group, topic, partition):
the offset of the next message to deliver. That number only moves forward, and
it covers a *range* -- so committing message N+1 implicitly acknowledges N,
whatever happened to N.

These tests pin down what the consumer does with that watermark when a message
fails to persist.
"""

from __future__ import annotations

import app.consumer

from tests.conftest import (
    FakeKafkaConsumer,
    FakeSession,
    make_database_error,
    make_event,
    make_message,
)


# Three consecutive offsets on the same partition. The middle one fails.
FIRST_OFFSET = 100
FAILING_OFFSET = 101
LAST_OFFSET = 102


def _patch_consumer_dependencies(monkeypatch, save_trade) -> None:
    """Replace Kafka and the database with doubles.

    Every name is patched on `app.consumer`, NOT on the module that defines it.
    `app/consumer.py` does `from confluent_kafka import Consumer` and
    `from app.database import SessionLocal, save_trade`, which binds three
    independent names in app.consumer's own namespace. Rebinding
    confluent_kafka.Consumer would leave app.consumer.Consumer untouched, the
    real client would be constructed, and the test would hang trying to reach
    a broker that isn't there.
    """
    monkeypatch.setattr(app.consumer, "Consumer", FakeKafkaConsumer)
    monkeypatch.setattr(app.consumer, "SessionLocal", lambda: FakeSession())
    monkeypatch.setattr(app.consumer, "save_trade", save_trade)


def test_healthy_run_commits_every_offset(monkeypatch):
    """Baseline: when every write succeeds, every offset is committed.

    This test exists to prove the harness itself is wired correctly. Without
    it, a red test below could be red because the fakes are broken rather than
    because the consumer is.
    """
    _patch_consumer_dependencies(monkeypatch, save_trade=lambda session, event: True)

    consumer = app.consumer.TradeStorageConsumer()

    events = [make_event(), make_event(), make_event()]
    offsets = [FIRST_OFFSET, FAILING_OFFSET, LAST_OFFSET]

    for offset, event in zip(offsets, events):
        consumer._process_message(make_message(offset, event))

    assert consumer.consumer.committed_offsets == offsets


def test_offset_watermark_does_not_advance_past_a_failed_message(monkeypatch):
    """A message that failed to persist must not be acknowledged by a later one.

    Scenario: PostgreSQL is unavailable for exactly one message. The consumer
    correctly declines to commit that message's offset -- but then processes
    the next one successfully and commits *its* offset, dragging the watermark
    past the failure.

    Kafka then reports LAG 0. The trade is not stored, not retried, and not
    recoverable without a manual consumer-group offset reset.
    """
    failing_event = make_event(symbol="MSFT")

    def save_trade(session, event):
        if event.event_id == failing_event.event_id:
            raise make_database_error()
        return True

    _patch_consumer_dependencies(monkeypatch, save_trade=save_trade)

    consumer = app.consumer.TradeStorageConsumer()

    messages = [
        make_message(FIRST_OFFSET, make_event(symbol="AAPL")),
        make_message(FAILING_OFFSET, failing_event),
        make_message(LAST_OFFSET, make_event(symbol="NVDA")),
    ]

    for message in messages:
        consumer._process_message(message)

    committed = consumer.consumer.committed_offsets

    # The failed message itself is correctly never committed. Asserting only
    # this would pass against the current code and prove nothing.
    assert FAILING_OFFSET not in committed

    # The real invariant: no commit may move the watermark to or beyond the
    # failed offset, because Kafka has no way to represent "everything up to
    # 102 except 101".
    highest_committed = max(committed, default=-1)

    assert highest_committed < FAILING_OFFSET, (
        f"Offset watermark advanced to {highest_committed}, past the failed "
        f"message at offset {FAILING_OFFSET}. Kafka now considers that message "
        f"processed: it will never be redelivered to this consumer group, and "
        f"lag will read 0 while the trade is missing from the database."
    )
