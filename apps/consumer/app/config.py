from market_core.config import DatabaseSettings, KafkaSettings


class Settings(KafkaSettings, DatabaseSettings):
    kafka_group_id: str = "market-trades-storage-v1"


settings = Settings()
