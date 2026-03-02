"""SQL Server persistence logic for Kalshi events."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.event_record import EventRecord
from repositories.base_repository import BaseSQLRepository


class EventRepository(BaseSQLRepository):
    """Insert events when absent in SQL Server."""

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
        self.logger.debug("Prepared %s parameter sets for event insert-if-absent", len(rows))
        return self._executemany(self.insert_statement, rows)

    def update_event_fields(
        self,
        event_ticker: str,
        *,
        title: Optional[str] = None,
        sub_title: Optional[str] = None,
        collateral_return_type: Optional[str] = None,
        series_ticker: Optional[str] = None,
        strike_date: Optional[datetime] = None,
        strike_period: Optional[str] = None,
    ) -> int:
        assignments: list[str] = []
        params: list[object] = []
        if title is not None:
            assignments.append("title = ?")
            params.append(title)
        if sub_title is not None:
            assignments.append("sub_title = ?")
            params.append(sub_title)
        if collateral_return_type is not None:
            assignments.append("collateral_return_type = ?")
            params.append(collateral_return_type)
        if series_ticker is not None:
            assignments.append("series_ticker = ?")
            params.append(series_ticker)
        if strike_date is not None:
            assignments.append("strike_date = ?")
            params.append(strike_date)
        if strike_period is not None:
            assignments.append("strike_period = ?")
            params.append(strike_period)

        if not assignments:
            self.logger.debug("No event fields supplied for update; event_ticker=%s", event_ticker)
            return 0

        assignments.append("UpdateTime = ?")
        params.append(datetime.now())
        params.append(event_ticker)

        statement = f"UPDATE {self.table_name} SET " + ", ".join(assignments) + " WHERE event_ticker = ?"
        return self._execute_update(statement, params)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (" + ", ".join(["?"] * 13) + ")) AS source "
            "(event_ticker, series_ticker, category, title, sub_title, available_on_brokers, collateral_return_type, mutually_exclusive, strike_date, strike_period, last_updated_ts, AddTime, UpdateTime) "
            "ON target.event_ticker = source.event_ticker "
            "WHEN NOT MATCHED THEN INSERT "
            "(event_ticker, series_ticker, category, title, sub_title, available_on_brokers, collateral_return_type, mutually_exclusive, strike_date, strike_period, last_updated_ts, AddTime, UpdateTime) "
            "VALUES (source.event_ticker, source.series_ticker, source.category, source.title, source.sub_title, source.available_on_brokers, source.collateral_return_type, source.mutually_exclusive, source.strike_date, source.strike_period, source.last_updated_ts, source.AddTime, source.UpdateTime);"
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
