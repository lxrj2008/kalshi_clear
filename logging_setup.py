"""Central logging configuration for the Kalshi API framework."""
from __future__ import annotations

import logging
from pathlib import Path
from logging.config import dictConfig


def configure_logging(
    level: str = "INFO",
    log_dir: Path | str | None = None,
    filename: str = "kalshi.log",
) -> logging.Logger:
    """Configure application logging (console + daily file rotation)."""
    log_directory = Path(log_dir or "logs").expanduser().resolve()
    log_directory.mkdir(parents=True, exist_ok=True)
    log_file = log_directory / filename

    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s | %(levelname)s | %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": level.upper(),
                    "formatter": "standard",
                },
                "file": {
                    "class": "logging.handlers.TimedRotatingFileHandler",
                    "level": level.upper(),
                    "formatter": "standard",
                    "filename": str(log_file),
                    "when": "midnight",
                    "interval": 1,
                    "backupCount": 14,
                    "encoding": "utf-8",
                },
            },
            "root": {
                "handlers": ["console", "file"],
                "level": level.upper(),
            },
        }
    )
    return logging.getLogger("kalshi")

