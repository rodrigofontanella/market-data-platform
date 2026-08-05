import logging

from app.config import settings
from app.consumer import TradeStorageConsumer
from market_core.logging import configure_logging


configure_logging(
    service_name=settings.service_name,
    log_level=settings.log_level,
    log_format=settings.log_format,
)

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("consumer_process_starting")

    consumer = TradeStorageConsumer()
    consumer.run()


if __name__ == "__main__":
    main()