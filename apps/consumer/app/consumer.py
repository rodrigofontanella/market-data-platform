import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.config import settings
from app.database import SessionLocal, save_trade
from market_core import TradeEvent

import time

from app.metrics import (
    CONSUMER_FAILURES_TOTAL,
    CONSUMER_PROCESSING_DURATION_SECONDS,
    DUPLICATE_TRADES_TOTAL,
    INVALID_EVENTS_TOTAL,
    TRADES_CONSUMED_TOTAL,
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
            }
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

        finally:
            logger.info("consumer_stopping")
            self.consumer.close()
            logger.info("consumer_stopped")

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
            self.consumer.commit(
                message=message,
                asynchronous=False,
            )

            logger.warning(
                "invalid_event_offset_committed",
                extra=kafka_context,
            )

        except SQLAlchemyError:
            CONSUMER_FAILURES_TOTAL.labels(
                failure_type="database",
                    ).inc()

            logger.exception(
                "trade_database_failed",
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