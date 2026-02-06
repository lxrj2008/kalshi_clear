"""SQL Server persistence logic for Kalshi events."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.event_record import EventRecord
from repositories.base_repository import BaseSQLRepository


class EventRepository(BaseSQLRepository):
    """Insert or update events in SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Events",
    ) -> None:
        super().__init__(settings, logger=logger)
        self.table_name = table_name

    def save_events(self, records: Sequence[EventRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        self.logger.debug("Prepared %s parameter sets for event upsert", len(rows))
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (?, ?, ?, ?, ?, ?)) AS source "
            "(event_ticker, series_ticker, title, sub_title, add_time, update_time) "
            "ON target.event_ticker = source.event_ticker "
            "WHEN MATCHED THEN UPDATE SET "
            "title = source.title, "
            "sub_title = source.sub_title, "
            "series_ticker = source.series_ticker, "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(event_ticker, series_ticker, title, sub_title, AddTime, UpdateTime) "
            "VALUES (source.event_ticker, source.series_ticker, source.title, source.sub_title, source.add_time, source.update_time);"
        )

    def _build_row(self, record: EventRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.event_ticker,
            record.series_ticker,
            record.title,
            record.sub_title,
            add_time,
            update_time,
        )


__all__ = ["EventRepository"]
