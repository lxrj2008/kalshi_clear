"""Connection factory for SQL Server (pyodbc).

Why this exists:
- `pyodbc.connect()` can be relatively expensive when called frequently.
- This project has both a scheduler loop and a websocket listener thread; connections
  must NOT be shared across threads.

Strategy:
- Keep one live connection per-thread per database (thread-local cache).
- Provide a context manager that yields a connection without closing it on exit.
- If the connection looks dead or a DB operation fails with a connection error,
  callers can discard and recreate.

Notes:
- pyodbc has built-in pooling (`pyodbc.pooling`), but caching per thread further
  reduces Python-level connect churn and avoids accidental cross-thread sharing.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator, Optional

import pyodbc

from config import KalshiSettings


class OdbcConnectionFactory:
    """Thread-local connection cache keyed by database name."""

    def __init__(self, settings: KalshiSettings) -> None:
        self._settings = settings
        self._local = threading.local()

        # Ensure pooling is enabled (pyodbc defaults True, but be explicit).
        pyodbc.pooling = True

    def _cache(self) -> dict[str, pyodbc.Connection]:
        cache = getattr(self._local, "connections", None)
        if cache is None:
            cache = {}
            self._local.connections = cache
        return cache

    def get(self, database_name: Optional[str] = None) -> pyodbc.Connection:
        """Return a cached connection for this thread (create on first use)."""
        db = database_name or self._settings.sqlserver_database
        cache = self._cache()
        conn = cache.get(db)
        if conn is not None:
            return conn
        connection_string = self._settings.build_sqlserver_connection_string(db)
        conn = pyodbc.connect(connection_string)
        cache[db] = conn
        return conn

    def discard(self, database_name: Optional[str] = None) -> None:
        """Close and remove cached connection for this thread/database."""
        db = database_name or self._settings.sqlserver_database
        cache = self._cache()
        conn = cache.pop(db, None)
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass

    @contextmanager
    def connection(self, database_name: Optional[str] = None) -> Iterator[pyodbc.Connection]:
        """Context manager yielding a reusable connection (does not close on exit)."""
        yield self.get(database_name)


__all__ = ["OdbcConnectionFactory"]

