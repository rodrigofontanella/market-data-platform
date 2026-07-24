import logging

from confluent_kafka import Producer

from market_core import TradeEvent

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

    @staticmethod
    def _delivery_report(error, message) -> None:
        if error is not None:
            logger.error("Message delivery failed: %s", error)
            return

        logger.info(
            "Message delivered to topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def publish_trade(self, trade: TradeEvent) -> None:
        payload = trade.model_dump_json()

        self.producer.produce(
            topic=self.topic,
            key=trade.symbol,
            value=payload,
            callback=self._delivery_report,
        )

        # Triggers delivery callbacks without blocking for all messages.
        self.producer.poll(0)

    def close(self) -> None:
        logger.info("Flushing pending Kafka messages")
        self.producer.flush(10)