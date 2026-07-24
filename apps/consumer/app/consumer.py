import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.config import settings
from app.database import SessionLocal, save_trade
from market_core import TradeEvent

logger = logging.getLogger(__name__)


class TradeStorageConsumer:
    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "session.timeout.ms": 45000,
                "heartbeat.interval.ms": 15000,
                "max.poll.interval.ms": 300000,
                "socket.timeout.ms": 60000,
            }
        )

    def run(self) -> None:
        self.consumer.subscribe([settings.kafka_topic])

        logger.info(
            "Consumer started: topic=%s group=%s",
            settings.kafka_topic,
            settings.kafka_group_id,
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
            logger.info("Consumer stopped by user")

        except KafkaException:
            logger.exception("Kafka consumer error")

        finally:
            logger.info("Closing Kafka consumer")
            self.consumer.close()

    def _process_message(self, message: Message) -> None:
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
                logger.info(
                    "Stored trade: symbol=%s price=%s event_id=%s",
                    event.symbol,
                    event.price,
                    event.event_id,
                )
            else:
                logger.warning(
                    "Duplicate event ignored: event_id=%s",
                    event.event_id,
                )

            # Commit only after the PostgreSQL transaction succeeds.
            self.consumer.commit(
                message=message,
                asynchronous=False,
            )

        except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as error:
            logger.error(
                "Invalid event at partition=%s offset=%s: %s",
                message.partition(),
                message.offset(),
                error,
            )

            # Temporary policy: skip malformed events.
            self.consumer.commit(
                message=message,
                asynchronous=False,
            )

        except SQLAlchemyError:
            logger.exception(
                "Database error at partition=%s offset=%s. "
                "Kafka offset was not committed.",
                message.partition(),
                message.offset(),
            )

        except KafkaException:
            logger.exception(
                "Kafka commit error at partition=%s offset=%s",
                message.partition(),
                message.offset(),
            )
            raise