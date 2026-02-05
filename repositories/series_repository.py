"""Utilities for persisting Kalshi series data into SQL Server."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.series_record import SeriesRecord
from repositories.base_repository import BaseSQLRepository


class SeriesRepository(BaseSQLRepository):
    """Handle bulk persistence of `SeriesRecord` rows."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Series",
    ) -> None:
        super().__init__(settings, logger=logger)
        self.table_name = table_name

    def save_series(self, records: Sequence[SeriesRecord]) -> int:
        params = [
            (
                record.ticker,
                record.title,
                record.category,
                record.status,
                record.add_time or datetime.utcnow(),
                record.update_time or datetime.utcnow(),
            )
            for record in records
        ]
        self.logger.debug("Prepared %s parameter sets for series insert", len(params))
        return self.save_many(params)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"INSERT INTO {self.table_name} (ticker, title, category, status, AddTime, UpdateTime) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )


__all__ = ["SeriesRepository"]
