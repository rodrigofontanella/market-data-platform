import logging

from confluent_kafka import Message, Producer
from market_core import TradeEvent

from app.metrics import (
    PRODUCER_PUBLISH_FAILURES_TOTAL,
    TRADES_PUBLISHED_TOTAL,
)

logger = logging.getLogger(__name__)


class MarketDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "market-data-producer",
                "acks": "all",
            }
        )

    def _delivery_report(
        self,
        error,
        message: Message,
    ) -> None:
        symbol = (
            message.key().decode("utf-8")
            if message.key()
            else "unknown"
        )

        if error is not None:
            PRODUCER_PUBLISH_FAILURES_TOTAL.labels(
                failure_type="delivery",
            ).inc()

            logger.error(
                "trade_delivery_failed",
                extra={
                    "symbol": symbol,
                    "topic": message.topic(),
                    "error": str(error),
                },
            )
            return

        TRADES_PUBLISHED_TOTAL.labels(
            symbol=symbol,
            topic=message.topic(),
        ).inc()

        logger.info(
            "trade_delivered",
            extra={
                "symbol": symbol,
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            },
        )

    def publish_trade(self, trade: TradeEvent) -> None:
        payload = trade.model_dump_json()

        self.producer.produce(
            topic=self.topic,
            key=trade.symbol,
            value=payload,
            callback=self._delivery_report,
        )

        self.producer.poll(0)

    def close(self) -> None:
        logger.info("producer_flushing")

        remaining = self.producer.flush(10)

        if remaining > 0:
            logger.warning(
                "producer_flush_incomplete",
                extra={
                    "undelivered_messages": remaining,
                },
            )
        else:
            logger.info("producer_flushed")