"""SQL Server persistence logic for Kalshi events."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.event_record import EventRecord
from repositories.base_repository import BaseSQLRepository, DatabaseSaveError


class EventRepository(BaseSQLRepository):
    """Insert events when absent in SQL Server."""

    COLUMNS: list[str] = [
        "event_ticker",
        "series_ticker",
        "category",
        "title",
        "sub_title",
        "available_on_brokers",
        "collateral_return_type",
        "mutually_exclusive",
        "strike_date",
        "strike_period",
        "last_updated_ts",
        "AddTime",
        "UpdateTime",
    ]

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Events",
        staging_table: str = "dbo.KS_Events_TEMP",
    ) -> None:
        super().__init__(settings, logger=logger)
        self.table_name = table_name
        self.staging_table = staging_table

    def save_events(self, records: Sequence[EventRecord], *, manage_truncate: bool = True) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        if manage_truncate:
            self._truncate_staging()
        batch_size = 10000
        for index in range(0, len(rows), batch_size):
            batch = rows[index : index + batch_size]
            self._executemany(self._staging_insert_statement, batch)
        affected = self._merge_from_staging(total=len(rows))
        self._truncate_staging()
        return affected

    @property
    def insert_statement(self) -> str:  
        columns = ", ".join(self.COLUMNS)
        placeholders = ", ".join(["?"] * len(self.COLUMNS))
        source_values = ", ".join([f"source.{name}" for name in self.COLUMNS])
        return (
            f"MERGE {self.table_name} AS target "
            f"USING (VALUES ({placeholders})) AS source ({columns}) "
            "ON target.event_ticker = source.event_ticker "
            "WHEN NOT MATCHED THEN INSERT "
            f"({columns}) VALUES ({source_values});"
        )

    @property
    def _staging_insert_statement(self) -> str:
        columns = ", ".join(self.COLUMNS)
        placeholders = ", ".join(["?"] * len(self.COLUMNS))
        return f"INSERT INTO {self.staging_table} ({columns}) VALUES ({placeholders})"

    @property
    def _merge_from_staging_statement(self) -> str:
        columns = ", ".join(self.COLUMNS)
        source_values = ", ".join([f"source.{name}" for name in self.COLUMNS])
        return (
            f"MERGE {self.table_name} AS target "
            f"USING {self.staging_table} AS source ON target.event_ticker = source.event_ticker "
            "WHEN NOT MATCHED THEN INSERT "
            f"({columns}) VALUES ({source_values});"
        )

    def _truncate_staging(self) -> None:
        self._execute_update(f"TRUNCATE TABLE {self.staging_table}", [], log_result=False)

    def _merge_from_staging(self, *, total: int) -> int:
        try:
            with self._connection() as connection:
                cursor = connection.cursor()
                cursor.execute(self._merge_from_staging_statement)
                rowcount = cursor.rowcount
                connection.commit()
        except Exception as exc:  
            self.logger.error("Merge from staging failed: %s", exc)
            raise DatabaseSaveError("Unable to persist event rows to SQL Server") from exc
        affected = rowcount if rowcount >= 0 else total
        self.logger.info("Merged %s event rows from staging", affected)
        return affected

    def reset_staging(self) -> None:
        """Explicitly truncate the staging table; callers can control boundaries."""
        self._truncate_staging()

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
