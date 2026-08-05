from market_core.config import DatabaseSettings


class Settings(DatabaseSettings):
    service_name: str = "consumer"

    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic: str = "market.trades.raw"
    kafka_group_id: str = "market-trades-storage-v1"


settings = Settings()