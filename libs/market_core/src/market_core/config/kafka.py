from market_core.config.base import BaseServiceSettings


class KafkaSettings(BaseServiceSettings):
    """Shared Kafka connection settings."""

    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic: str = "market.trades.raw"
