import logging

from app.consumer import TradeStorageConsumer
from app.database import create_tables

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)


def main() -> None:
    create_tables()

    consumer = TradeStorageConsumer()
    consumer.run()


if __name__ == "__main__":
    main()