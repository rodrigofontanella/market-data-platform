from prometheus_client import Counter, Histogram


TRADES_CONSUMED_TOTAL = Counter(
    "trades_consumed_total",
    "Total number of Kafka trade events received by the consumer",
    labelnames=["topic"],
)


TRADES_STORED_TOTAL = Counter(
    "trades_stored_total",
    "Total number of trade events successfully stored in PostgreSQL",
    labelnames=["symbol"],
)


DUPLICATE_TRADES_TOTAL = Counter(
    "duplicate_trades_total",
    "Total number of duplicate trade events ignored",
)


INVALID_EVENTS_TOTAL = Counter(
    "invalid_events_total",
    "Total number of invalid Kafka events",
    labelnames=["error_type"],
)


CONSUMER_FAILURES_TOTAL = Counter(
    "consumer_failures_total",
    "Total number of consumer processing failures",
    labelnames=["failure_type"],
)


CONSUMER_PROCESSING_DURATION_SECONDS = Histogram(
    "consumer_processing_duration_seconds",
    "Time spent processing a Kafka message",
)


TRADES_DEAD_LETTERED_TOTAL = Counter(
    "trades_dead_lettered_total",
    "Total number of messages published to the dead-letter topic",
    labelnames=["reason"],
)


DEAD_LETTER_FAILURES_TOTAL = Counter(
    "dead_letter_failures_total",
    "Total number of failures publishing to the dead-letter topic. "
    "Non-zero means messages could not be parked and the consumer crashed "
    "rather than dropping them.",
)