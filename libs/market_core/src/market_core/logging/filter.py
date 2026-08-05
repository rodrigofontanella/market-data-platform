from __future__ import annotations

import logging

from market_core.logging.context import get_request_id


class RequestContextFilter(logging.Filter):
    """Attach request-scoped context to every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        return True