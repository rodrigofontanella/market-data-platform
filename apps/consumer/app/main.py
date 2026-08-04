import logging

from app.consumer import TradeStorageConsumer

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)


def main() -> None:
    consumer = TradeStorageConsumer()
    consumer.run()


if __name__ == "__main__":
    main()