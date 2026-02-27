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
            "USING (VALUES (" + ", ".join(["?"] * 13) + ")) AS source "
            "(event_ticker, series_ticker, category, title, sub_title, available_on_brokers, collateral_return_type, mutually_exclusive, strike_date, strike_period, last_updated_ts, add_time, update_time) "
            "ON target.event_ticker = source.event_ticker "
            "WHEN MATCHED THEN UPDATE SET "
            "series_ticker = source.series_ticker, "
            "category = source.category, "
            "title = source.title, "
            "sub_title = source.sub_title, "
            "available_on_brokers = source.available_on_brokers, "
            "collateral_return_type = source.collateral_return_type, "
            "mutually_exclusive = source.mutually_exclusive, "
            "strike_date = source.strike_date, "
            "strike_period = source.strike_period, "
            "last_updated_ts = source.last_updated_ts, "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(event_ticker, series_ticker, category, title, sub_title, available_on_brokers, collateral_return_type, mutually_exclusive, strike_date, strike_period, last_updated_ts, AddTime, UpdateTime) "
            "VALUES (source.event_ticker, source.series_ticker, source.category, source.title, source.sub_title, source.available_on_brokers, source.collateral_return_type, source.mutually_exclusive, source.strike_date, source.strike_period, source.last_updated_ts, source.add_time, source.update_time);"
        )

    def _build_row(self, record: EventRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.event_ticker,
            record.series_ticker,
            record.category,
            record.title,
            record.sub_title,
            record.available_on_brokers,
            record.collateral_return_type,
            record.mutually_exclusive,
            record.strike_date,
            record.strike_period,
            record.last_updated_ts,
            add_time,
            update_time,
        )


__all__ = ["EventRepository"]
