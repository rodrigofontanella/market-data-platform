import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError
from sqlalchemy.exc import (
    InterfaceError,
    OperationalError,
    SQLAlchemyError,
)

from app.config import settings
from app.database import SessionLocal, save_trade
from app.dlq import DeadLetterProducer, DeadLetterPublishError
from market_core import TradeEvent

import time

from app.metrics import (
    CONSUMER_FAILURES_TOTAL,
    CONSUMER_PROCESSING_DURATION_SECONDS,
    DEAD_LETTER_FAILURES_TOTAL,
    DUPLICATE_TRADES_TOTAL,
    INVALID_EVENTS_TOTAL,
    TRADES_CONSUMED_TOTAL,
    TRADES_DEAD_LETTERED_TOTAL,
    TRADES_STORED_TOTAL,
)


logger = logging.getLogger(__name__)


class TradeStorageConsumer:
    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45_000,
                "heartbeat.interval.ms": 15_000,
                "max.poll.interval.ms": 300_000,
                "socket.timeout.ms": 60_000,
                "client.id": "market-data-consumer",
            }
        )

        # This service is now a producer as well as a consumer.
        self.dead_letters = DeadLetterProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_dlq_topic,
            flush_timeout_seconds=settings.dlq_flush_timeout_seconds,
        )

    def run(self) -> None:
        self.consumer.subscribe([settings.kafka_topic])

        logger.info(
            "consumer_started",
            extra={
                "topic": settings.kafka_topic,
                "consumer_group": settings.kafka_group_id,
                "brokers": settings.kafka_bootstrap_servers,
            },
        )

        try:
            while True:
                message = self.consumer.poll(timeout=1.0)

                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue

                    raise KafkaException(message.error())

                self._process_message(message)

        except KeyboardInterrupt:
            logger.info("consumer_interrupted")

        except KafkaException:
            logger.exception(
                "consumer_kafka_failed",
                extra={
                    "topic": settings.kafka_topic,
                    "consumer_group": settings.kafka_group_id,
                },
            )
            raise

        # Deliberate crash, not an accident. Logged explicitly so an operator
        # reading the logs can tell an intentional restart-to-retry apart from
        # an unhandled bug.
        except (OperationalError, InterfaceError):
            logger.exception(
                "consumer_restarting_after_database_outage",
                extra={
                    "topic": settings.kafka_topic,
                    "consumer_group": settings.kafka_group_id,
                },
            )
            raise

        finally:
            logger.info("consumer_stopping")
            self.consumer.close()
            logger.info("consumer_stopped")


    def _dead_letter(
        self,
        message: Message,
        error: Exception,
        reason: str,
        kafka_context: dict[str, Any],
    ) -> None:
        """Park an unprocessable message, or refuse to continue.

        Returns normally only once the DLQ write is CONFIRMED. The caller may
        commit the source offset after that and not before -- an enqueued but
        unflushed message exists only in this process's memory.
        """
        try:
            self.dead_letters.publish(
                message=message,
                error=error,
                reason=reason,
                consumer_group=settings.kafka_group_id,
            )

        except DeadLetterPublishError:
            DEAD_LETTER_FAILURES_TOTAL.inc()

            logger.exception(
                "dead_letter_publish_failed",
                extra={**kafka_context, "reason": reason},
            )

            raise

        TRADES_DEAD_LETTERED_TOTAL.labels(
            reason=reason,
        ).inc()

        logger.warning(
            "trade_dead_lettered",
            extra={
                **kafka_context,
                "reason": reason,
                "dlq_topic": settings.kafka_dlq_topic,
                "error_type": type(error).__name__,
            },
        )

    def _process_message(self, message: Message) -> None:
        started_at = time.perf_counter()

        kafka_context = {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "consumer_group": settings.kafka_group_id,
        }

        TRADES_CONSUMED_TOTAL.labels(
            topic=message.topic(),
                ).inc()

        try:
            payload: Any = json.loads(
                message.value().decode("utf-8")
            )

            event = TradeEvent.model_validate(payload)

            with SessionLocal() as session:
                try:
                    inserted = save_trade(session, event)
                    session.commit()

                except SQLAlchemyError:
                    session.rollback()
                    raise

            if inserted:
                TRADES_STORED_TOTAL.labels(
                    symbol=event.symbol,
                        ).inc()
                logger.info(
                    "trade_stored",
                    extra={
                        **kafka_context,
                        "event_id": str(event.event_id),
                        "symbol": event.symbol,
                        "price": event.price,
                        "volume": event.volume,
                        "schema_version": event.schema_version,
                    },
                )
            else:
                DUPLICATE_TRADES_TOTAL.inc()

                logger.warning(
                    "duplicate_trade_ignored",
                    extra={
                        **kafka_context,
                        "event_id": str(event.event_id),
                        "symbol": event.symbol,
                    },
                )

            # Commit only after the PostgreSQL transaction succeeds.
            self.consumer.commit(
                message=message,
                asynchronous=False,
            )

            logger.debug(
                "kafka_offset_committed",
                extra={
                    **kafka_context,
                    "event_id": str(event.event_id),
                },
            )


            # The payload is not a trade event and never will be. Park it, then
            # skip it. _dead_letter() raises rather than returning if the DLQ write
            # cannot be confirmed, so the commit below is unreachable in that case.


        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            ValidationError,
            ) as error:
            
            INVALID_EVENTS_TOTAL.labels(
                error_type=type(error).__name__,
                    ).inc()
            logger.error(
                "invalid_trade_event",
                extra={
                    **kafka_context,
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
            )

            # Temporary policy: malformed records are skipped.
            self._dead_letter(
                message=message,
                error=error,
                reason="invalid_payload",
                kafka_context=kafka_context,
            )

            self.consumer.commit(
                message=message,
                asynchronous=False,
            )

            logger.warning(
                "invalid_event_offset_committed",
                extra=kafka_context,
            )

        # Transient: the database is unreachable, the message is fine.
        #
        # The offset is deliberately NOT committed and the exception is
        # re-raised, which terminates the process. `run()`'s finally block
        # leaves the consumer group cleanly, and Compose's
        # `restart: unless-stopped` brings the container back. Because the
        # offset never advanced, the restarted consumer resumes at exactly
        # this message. The restart IS the retry.
        #
        # Redelivery is safe because save_trade() is idempotent on event_id
        # (ON CONFLICT DO NOTHING), so a replay produces duplicates rather
        # than duplicate rows.
        except (OperationalError, InterfaceError):
            CONSUMER_FAILURES_TOTAL.labels(
                failure_type="database_transient",
            ).inc()

            logger.exception(
                "trade_database_unavailable",
                extra=kafka_context,
            )

            raise

        # Permanent: the database is fine, this row cannot be written.
        # Retrying is pointless -- it would block the partition forever -- so
        # the message is skipped and its offset committed explicitly.
        #
        # NOTE: this is a data-loss path. It is the same "skip it" policy
        # already applied to malformed payloads above, and it is what the
        # dead-letter topic is intended to replace. Until then, the counter
        # and the log line are the only record that the trade existed.
        except SQLAlchemyError as error:
            CONSUMER_FAILURES_TOTAL.labels(
                failure_type="database_permanent",
            ).inc()

            logger.exception(
                "trade_write_rejected",
                extra=kafka_context,
            )

            self._dead_letter(
                message=message,
                error=error,
                reason="database_rejected",
                kafka_context=kafka_context,
            )

            self.consumer.commit(
                message=message,
                asynchronous=False,
            )

            logger.warning(
                "rejected_trade_offset_committed",
                extra=kafka_context,
            )

        except KafkaException:
            CONSUMER_FAILURES_TOTAL.labels(
                failure_type="kafka",
                    ).inc()

            logger.exception(
                "kafka_offset_commit_failed",
                extra=kafka_context,
            )
            raise

        finally:
            duration_seconds = time.perf_counter() - started_at

            CONSUMER_PROCESSING_DURATION_SECONDS.observe(
                duration_seconds
    )