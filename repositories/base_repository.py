"""Shared SQL Server persistence helpers for Kalshi data."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Iterator, Sequence

import pyodbc

from config import KalshiSettings
from db.connection_factory import OdbcConnectionFactory


class DatabaseSaveError(RuntimeError):
    """Raised when writing to SQL Server fails."""


class BaseSQLRepository(ABC):
    """Provide a consistent way to write batched rows into SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: logging.Logger | None = None,
        database_name: str | None = None,
    ) -> None:
        self.settings = settings
        self.database_name = database_name
        base_logger = logger or logging.getLogger("kalshi")
        self.logger = base_logger.getChild(self.__class__.__name__.lower())
        self._connection_factory = OdbcConnectionFactory(settings)

    def _executemany(self, statement: str, rows: Sequence[tuple[object, ...]]) -> int:
        try:
            with self._connection() as connection:
                cursor = connection.cursor()
                cursor.fast_executemany = True
                cursor.executemany(statement, rows)
                rowcount = cursor.rowcount
                connection.commit()
        except pyodbc.Error as exc:  
            self.logger.error("Bulk insert failed: %s", exc)
            raise DatabaseSaveError("Unable to persist rows to SQL Server") from exc
        affected = rowcount if rowcount >= 0 else len(rows)
        self.logger.info("Inserted %s rows", affected)
        return affected

    def _execute_update(self, statement: str, params: Sequence[object], *, log_result: bool = True) -> int:
        try:
            with self._connection() as connection:
                cursor = connection.cursor()
                cursor.execute(statement, params)
                connection.commit()
                rowcount = cursor.rowcount
        except pyodbc.Error as exc:  
            self.logger.error("Update failed: %s", exc)
            raise DatabaseSaveError("Unable to persist rows to SQL Server") from exc
        if log_result:
            self.logger.info("Updated %s rows", rowcount)
        return rowcount

    @contextmanager
    def _connection(self) -> Iterator[pyodbc.Connection]:
        """Yield a reusable per-thread connection (not closed on exit)."""
        try:
            with self._connection_factory.connection(self.database_name) as conn:
                yield conn
        except pyodbc.Error as exc:
            self._connection_factory.discard(self.database_name)
            raise exc

    @property
    @abstractmethod
    def insert_statement(self) -> str:
        """Return the INSERT statement used for bulk writes."""
        raise NotImplementedError


__all__ = ["BaseSQLRepository", "DatabaseSaveError"]
