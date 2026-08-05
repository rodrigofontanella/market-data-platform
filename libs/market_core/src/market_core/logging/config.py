from __future__ import annotations

import logging
import logging.config
from typing import Literal

from market_core.logging.formatter import JsonFormatter
from market_core.logging.filter import RequestContextFilter


LogFormat = Literal["console", "json"]


def configure_logging(
    *,
    service_name: str,
    log_level: str = "INFO",
    log_format: LogFormat = "console",
) -> None:
    """
    Configure process-wide logging for one service.

    This function should be called once by each executable application
    during startup.
    """

    normalized_level = log_level.upper()

    if normalized_level not in {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }:
        raise ValueError(f"Unsupported log level: {log_level}")

    formatters: dict[str, dict[str, object]] = {
        "console": {
            "format": (
                "%(asctime)s | %(levelname)s | "
                f"{service_name} | %(name)s | "
                "request_id=%(request_id)s | %(message)s"
        ),
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "json": {
            "()": JsonFormatter,
            "service_name": service_name,
        },
    }

    logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "request_context": {
            "()": RequestContextFilter,
        },
    },
    "formatters": formatters,
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": log_format,
            "filters": ["request_context"],
            }
        },
        "root": {
            "level": normalized_level,
            "handlers": ["stdout"],
        },
        "loggers": {
            "uvicorn": {
                "level": normalized_level,
                "handlers": ["stdout"],
                "propagate": False,
            },
            "uvicorn.error": {
                "level": normalized_level,
                "handlers": ["stdout"],
                "propagate": False,
            },
            "uvicorn.access": {
                "level": "WARNING",
                "handlers": ["stdout"],
                "propagate": False,
},
            "sqlalchemy.engine": {
                "level": "WARNING",
                "handlers": ["stdout"],
                "propagate": False,
            },
            "kafka": {
                "level": "WARNING",
                "handlers": ["stdout"],
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(logging_config)