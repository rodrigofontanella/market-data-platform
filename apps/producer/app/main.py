import logging
import random
import time
from datetime import UTC, datetime

from app.config import settings
from app.producer import MarketDataProducer
from market_core import TradeEvent
from market_core.logging import configure_logging


configure_logging(
    service_name=settings.service_name,
    log_level=settings.log_level,
    log_format=settings.log_format,
)

logger = logging.getLogger(__name__)


BASE_PRICES = {
    "AAPL": 215.00,
    "MSFT": 510.00,
    "NVDA": 172.00,
}


def generate_trade(symbol: str) -> TradeEvent:
    base_price = BASE_PRICES[symbol]
    price_change = random.uniform(-1.0, 1.0)

    return TradeEvent(
        symbol=symbol,
        price=round(base_price + price_change, 2),
        volume=random.randint(1, 1_000),
        timestamp=datetime.now(UTC),
        source="simulator",
    )


def run() -> None:
    producer = MarketDataProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
    )

    logger.info(
        "producer_starting",
        extra={
            "brokers": settings.kafka_bootstrap_servers,
            "topic": settings.kafka_topic,
            "interval_seconds": settings.producer_interval_seconds,
        },
    )

    try:
        while True:
            symbol = random.choice(list(BASE_PRICES))
            trade = generate_trade(symbol)

            producer.publish_trade(trade)

            logger.info(
                "trade_published",
                extra={
                    "event_id": str(trade.event_id),
                    "symbol": trade.symbol,
                    "price": trade.price,
                    "volume": trade.volume,
                    "topic": settings.kafka_topic,
                    "schema_version": trade.schema_version,
                },
            )

            time.sleep(settings.producer_interval_seconds)

    except KeyboardInterrupt:
        logger.info("producer_interrupted")

    except Exception:
        logger.exception(
            "producer_failed",
            extra={
                "topic": settings.kafka_topic,
                "brokers": settings.kafka_bootstrap_servers,
            },
        )
        raise

    finally:
        logger.info("producer_stopping")
        producer.close()
        logger.info("producer_stopped")


if __name__ == "__main__":
    run()