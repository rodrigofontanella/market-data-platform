from market_core.config import DatabaseSettings


class Settings(DatabaseSettings):
    service_name: str = "consumer"

    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic: str = "market.trades.raw"
    kafka_group_id: str = "market-trades-storage-v1"

    # Messages that can never be processed are published here instead of being
    # silently skipped. Retention on this topic should be LONGER than on the
    # source topic -- the point is having time to notice before the evidence
    # expires.
    kafka_dlq_topic: str = "market.trades.dlq"

    # flush() blocks the poll loop, so this must stay well under
    # max.poll.interval.ms (300s).
    dlq_flush_timeout_seconds: float = 5.0


settings = Settings()