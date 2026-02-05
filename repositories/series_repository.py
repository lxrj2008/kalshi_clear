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
        rows = [self._build_row(record) for record in records]
        self.logger.debug("Prepared %s parameter sets for series upsert", len(rows))
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (?, ?, ?, ?, ?, ?)) AS source "
            "(ticker, title, category, status, add_time, update_time) "
            "ON target.ticker = source.ticker "
            "WHEN MATCHED THEN UPDATE SET "
            "title = source.title, "
            "category = source.category, "
            "status = source.status, "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(ticker, title, category, status, AddTime, UpdateTime) "
            "VALUES (source.ticker, source.title, source.category, source.status, source.add_time, source.update_time);"
        )

    def _build_row(self, record: SeriesRecord) -> tuple[object, ...]:
        current_time = datetime.now()
        add_time = record.add_time or current_time
        update_time = record.update_time or current_time
        return (
            record.ticker,
            record.title,
            record.category,
            record.status,
            add_time,
            update_time,
        )


__all__ = ["SeriesRepository"]
