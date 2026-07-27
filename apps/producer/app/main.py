import logging
import random
import time
from datetime import datetime, timezone

from app.config import settings
from app.producer import MarketDataProducer
from market_core import TradeEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
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
        timestamp=datetime.now(timezone.utc),
        source="simulator",
    )


def run() -> None:
    producer = MarketDataProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_topic,
    )

    logger.info(
        "Starting producer: brokers=%s topic=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
    )

    try:
        while True:
            symbol = random.choice(list(BASE_PRICES))
            trade = generate_trade(symbol)

            producer.publish_trade(trade)

            logger.info(
                "Published trade: symbol=%s price=%s volume=%s",
                trade.symbol,
                trade.price,
                trade.volume,
            )

            time.sleep(settings.producer_interval_seconds)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        producer.close()


if __name__ == "__main__":
    run()