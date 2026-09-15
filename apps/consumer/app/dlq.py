"""Dead-letter publishing for the storage consumer.

A dead-letter topic is not error handling -- it is a second write path, with
its own producer, its own delivery semantics and its own failure modes.

The message published here carries the ORIGINAL BYTES, untouched. Not the
parsed TradeEvent: the failure class that matters most is the payload that
never parsed, so there is nothing to re-serialise. Keeping the value
byte-identical also makes replay trivial -- read the value, produce it back to
the source topic -- and preserves the exact bytes as evidence when debugging
why validation rejected something.

Context therefore goes in Kafka headers rather than being merged into the
payload, because you cannot merge JSON into something that is not valid JSON.

Delivery semantics: AT-LEAST-ONCE. A crash between a confirmed DLQ publish and
the source-topic offset commit replays the message, and the DLQ has no
idempotency key, so it can contain duplicates. Any replay tool must tolerate
that -- which it does for free, since replay produces back into the source
topic and save_trade() is idempotent on event_id.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from confluent_kafka import KafkaException, Message, Producer

logger = logging.getLogger(__name__)


# str(error) on a pydantic ValidationError can run to kilobytes. Headers are
# metadata, not a log sink.
MAX_ERROR_HEADER_BYTES = 1_000


class DeadLetterPublishError(RuntimeError):
    """The message could not be CONFIRMED written to the dead-letter topic.

    Distinct from "publish was attempted". An enqueued-but-unflushed message
    lives only in this process's memory, and this process is designed to die.
    """


class DeadLetterProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        flush_timeout_seconds: float = 5.0,
    ) -> None:
        self.topic = topic

        # Keep this SMALL relative to max.poll.interval.ms. flush() blocks the
        # poll loop, and a blocked poll loop stops the heartbeats that keep
        # this consumer in its group. flush() with no argument blocks forever,
        # which guarantees eviction whenever the broker is unwell.
        self.flush_timeout_seconds = flush_timeout_seconds

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "market-data-consumer-dlq",
                "acks": "all",
            }
        )

    def publish(
        self,
        message: Message,
        error: Exception,
        reason: str,
        consumer_group: str,
    ) -> None:
        """Publish a failed message and BLOCK until delivery is confirmed.

        Raises DeadLetterPublishError if delivery cannot be confirmed. The
        caller must not commit the source offset in that case -- doing so would
        acknowledge work that never happened, which is the bug this whole
        mechanism exists to prevent, one layer out.
        """
        headers = [
            ("x-original-topic", _encode(message.topic())),
            ("x-original-partition", _encode(message.partition())),
            ("x-original-offset", _encode(message.offset())),
            ("x-consumer-group", _encode(consumer_group)),
            ("x-reason", _encode(reason)),
            ("x-error-type", _encode(type(error).__name__)),
            ("x-error-message", _encode(str(error))[:MAX_ERROR_HEADER_BYTES]),
            ("x-failed-at", _encode(datetime.now(UTC).isoformat())),
        ]

        try:
            self.producer.produce(
                topic=self.topic,
                key=message.key(),
                value=message.value(),
                headers=headers,
            )

        except (BufferError, KafkaException, ValueError) as publish_error:
            raise DeadLetterPublishError(
                f"could not enqueue to {self.topic}: {publish_error}"
            ) from publish_error

        # produce() only enqueues. Until this returns 0, the message exists
        # nowhere but in librdkafka's in-memory buffer.
        remaining = self.producer.flush(self.flush_timeout_seconds)

        if remaining > 0:
            raise DeadLetterPublishError(
                f"{remaining} message(s) undelivered to {self.topic} after "
                f"{self.flush_timeout_seconds}s"
            )

    def close(self) -> None:
        remaining = self.producer.flush(self.flush_timeout_seconds)

        if remaining > 0:
            logger.error(
                "dead_letter_flush_incomplete",
                extra={"undelivered_messages": remaining, "topic": self.topic},
            )


def _encode(value: object) -> bytes:
    return str(value).encode("utf-8")