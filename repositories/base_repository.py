"""Shared SQL Server persistence helpers for Kalshi data."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Sequence

import pyodbc

from config import KalshiSettings


class DatabaseSaveError(RuntimeError):
    """Raised when writing to SQL Server fails."""


class BaseSQLRepository(ABC):
    """Provide a consistent way to write batched rows into SQL Server."""

    def __init__(self, settings: KalshiSettings, logger: logging.Logger | None = None) -> None:
        self.settings = settings
        base_logger = logger or logging.getLogger("kalshi")
        self.logger = base_logger.getChild(self.__class__.__name__.lower())

    def save_many(self, rows: Sequence[tuple[object, ...]]) -> int:
        """Persist rows using the concrete class's insert statement."""
        if not rows:
            self.logger.info("No rows supplied; skipping insert.")
            return 0
        statement = self.insert_statement
        try:
            with self._connect() as connection:
                cursor = connection.cursor()
                cursor.fast_executemany = True
                cursor.executemany(statement, rows)
                connection.commit()
        except pyodbc.Error as exc:  # pragma: no cover - depends on driver
            self.logger.error("Bulk insert failed: %s", exc)
            raise DatabaseSaveError("Unable to persist rows to SQL Server") from exc
        self.logger.info("Inserted %s rows", len(rows))
        return len(rows)

    def _connect(self) -> pyodbc.Connection:
        return pyodbc.connect(self.settings.sqlserver_connection_string)

    @property
    @abstractmethod
    def insert_statement(self) -> str:
        """Return the INSERT statement used for bulk writes."""
        raise NotImplementedError


__all__ = ["BaseSQLRepository", "DatabaseSaveError"]
