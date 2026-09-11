"""Offset-commit semantics for the storage consumer.

A committed offset is a partition-level watermark, not a per-message
acknowledgement. Kafka stores exactly one number per (group, topic, partition):
the offset of the next message to deliver. That number only moves forward, and
it covers a *range* -- so committing message N+1 implicitly acknowledges N,
whatever happened to N.

There is no way to express "everything up to 102 except 101". That is why a
failed write cannot simply be skipped: the consumer must either stop, or
deliberately decide the message is unprocessable and say so.

Delivery semantics pinned down by these tests: AT-LEAST-ONCE. On a transient
failure the consumer dies without committing, restarts, and reprocesses from
the failed offset -- so messages already written are seen twice. That is safe
only because save_trade() is idempotent on event_id.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError

import app.consumer

from tests.conftest import (
    FakeDeadLetterProducer,
    FakeKafkaConsumer,
    FakeSession,
    make_event,
    make_message,
    make_permanent_database_error,
    make_transient_database_error,
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
    monkeypatch.setattr(app.consumer, "DeadLetterProducer", lambda **kwargs: FakeDeadLetterProducer(event_log=[]),)


def test_healthy_run_commits_every_offset(monkeypatch):
    """Baseline: when every write succeeds, every offset is committed.

    This test exists to prove the harness itself is wired correctly, and to
    catch a fix that accidentally breaks the normal path while repairing the
    failure path. If this one goes red, the problem is not in error handling.
    """
    _patch_consumer_dependencies(monkeypatch, save_trade=lambda session, event: True)

    consumer = app.consumer.TradeStorageConsumer()

    offsets = [FIRST_OFFSET, FAILING_OFFSET, LAST_OFFSET]

    for offset in offsets:
        consumer._process_message(make_message(offset, make_event()))

    assert consumer.consumer.committed_offsets == offsets


def test_transient_failure_stops_the_consumer_without_advancing_the_watermark(
    monkeypatch,
):
    """A database outage must halt the consumer, not skip the message.

    The consumer cannot mark one message unprocessed while continuing past it,
    because the offset is a watermark. So it declines to commit and re-raises,
    terminating the process. The supervisor restarts it, and because the offset
    never moved, it resumes at exactly this message.

    Before the fix, this exception was swallowed and the loop continued -- the
    next successful message committed its own offset and dragged the watermark
    past the failure. Kafka reported LAG 0 while the trade was missing.
    """
    failing_event = make_event(symbol="MSFT")

    def save_trade(session, event):
        if event.event_id == failing_event.event_id:
            raise make_transient_database_error()
        return True

    _patch_consumer_dependencies(monkeypatch, save_trade=save_trade)

    consumer = app.consumer.TradeStorageConsumer()

    consumer._process_message(make_message(FIRST_OFFSET, make_event(symbol="AAPL")))

    # The consumer must refuse to continue rather than skip the message.
    with pytest.raises(OperationalError):
        consumer._process_message(make_message(FAILING_OFFSET, failing_event))

    committed = consumer.consumer.committed_offsets

    assert FAILING_OFFSET not in committed

    highest_committed = max(committed, default=-1)

    assert highest_committed < FAILING_OFFSET, (
        f"Offset watermark advanced to {highest_committed}, past the failed "
        f"message at offset {FAILING_OFFSET}. Kafka would consider that "
        f"message processed: never redelivered, and lag reading 0 while the "
        f"trade is missing from the database."
    )


def test_the_watermark_cannot_be_dragged_past_a_failed_message(monkeypatch):
    """The regression guard for the original bug.

    If someone later 'improves' the transient handler by swallowing the
    exception so the consumer 'keeps working', processing continues to the
    next message and its commit acknowledges the failure by implication.
    This test fails the moment that happens.
    """
    failing_event = make_event(symbol="MSFT")

    def save_trade(session, event):
        if event.event_id == failing_event.event_id:
            raise make_transient_database_error()
        return True

    _patch_consumer_dependencies(monkeypatch, save_trade=save_trade)

    consumer = app.consumer.TradeStorageConsumer()

    messages = [
        make_message(FIRST_OFFSET, make_event(symbol="AAPL")),
        make_message(FAILING_OFFSET, failing_event),
        make_message(LAST_OFFSET, make_event(symbol="NVDA")),
    ]

    with pytest.raises(OperationalError):
        for message in messages:
            consumer._process_message(message)

    assert consumer.consumer.committed_offsets == [FIRST_OFFSET], (
        "The consumer processed a message after a transient failure. Whatever "
        "it committed for that later message silently acknowledged the failed "
        "one."
    )


def test_permanent_failure_skips_the_message_and_commits_its_offset(monkeypatch):
    """A row the database will never accept must not block the partition.

    Retrying a DataError forever stalls the consumer group on one message. So
    this class is skipped deliberately and its offset committed, exactly as
    malformed payloads already are.

    This is a data-loss path, documented rather than hidden -- it is what the
    dead-letter topic is meant to replace. When the DLQ lands, this test should
    change to assert the message was published there before being committed.
    """
    poison_event = make_event(symbol="MSFT")

    def save_trade(session, event):
        if event.event_id == poison_event.event_id:
            raise make_permanent_database_error()
        return True

    _patch_consumer_dependencies(monkeypatch, save_trade=save_trade)

    consumer = app.consumer.TradeStorageConsumer()

    # Must NOT raise: a permanent failure is not a reason to stop the world.
    consumer._process_message(make_message(FIRST_OFFSET, make_event(symbol="AAPL")))
    consumer._process_message(make_message(FAILING_OFFSET, poison_event))
    consumer._process_message(make_message(LAST_OFFSET, make_event(symbol="NVDA")))

    assert consumer.consumer.committed_offsets == [
        FIRST_OFFSET,
        FAILING_OFFSET,
        LAST_OFFSET,
    ]
