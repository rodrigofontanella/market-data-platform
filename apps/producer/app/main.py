import logging
import random
import time
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal

from market_core import TradeEvent
from market_core.logging import configure_logging
from prometheus_client import start_http_server

from app.config import settings
from app.metrics import (
    PRODUCER_ENQUEUE_DURATION_SECONDS,
    PRODUCER_PUBLISH_FAILURES_TOTAL,
    TRADES_GENERATED_TOTAL,
)
from app.producer import MarketDataProducer

configure_logging(
    service_name=settings.service_name,
    log_level=settings.log_level,
    log_format=settings.log_format,
)

logger = logging.getLogger(__name__)

PRICE_EXPONENT = Decimal("0.01")

BASE_PRICES = {
    "AAPL": 215.00,
    "MSFT": 510.00,
    "NVDA": 172.00,
}


def generate_trade(symbol: str) -> TradeEvent:
    base_price = Decimal(BASE_PRICES[symbol])
    price_change = Decimal(str(random.uniform(-1.0, 1.0)))
    price = (base_price + price_change).quantize(
        PRICE_EXPONENT,
        rounding=ROUND_HALF_UP,
    )


    return TradeEvent(
        symbol=symbol,
        price=price,
        volume=random.randint(1, 1_000),
        timestamp=datetime.now(UTC),
        source="simulator",
    )


def run() -> None:

    start_http_server(9102)

    logger.info(
        "metrics_server_started",
        extra={"metrics_port": 9102},
)
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

    while True:
            symbol = random.choice(list(BASE_PRICES))
            trade = generate_trade(symbol)

            TRADES_GENERATED_TOTAL.labels(
                symbol=trade.symbol,
                    ).inc()

            publish_started_at = time.perf_counter()

            try:
                producer.publish_trade(trade)

            except Exception as error:
                PRODUCER_PUBLISH_FAILURES_TOTAL.labels(
                    failure_type=type(error).__name__,
                        ).inc()

                logger.exception(
                    "trade_publish_failed",
                    extra={
                        "event_id": str(trade.event_id),
                        "symbol": trade.symbol,
                        "topic": settings.kafka_topic,
                    },
                )

                raise

            else:
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

            finally:
                publish_duration_seconds = (
                    time.perf_counter() - publish_started_at
                )

                PRODUCER_ENQUEUE_DURATION_SECONDS.observe(
                    publish_duration_seconds
                )

            time.sleep(settings.producer_interval_seconds)



if __name__ == "__main__":
    run()