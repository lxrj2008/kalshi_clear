"""SQL Server persistence for sports."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.sport_record import SportRecord
from repositories.base_repository import BaseSQLRepository


class SportRepository(BaseSQLRepository):
    """Insert or update sports in SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Sports",
    ) -> None:
        super().__init__(settings, logger=logger)
        self.table_name = table_name

    def save_sports(self, records: Sequence[SportRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        self.logger.debug("Prepared %s parameter sets for sport upsert", len(rows))
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (?, ?, ?)) AS source "
            "(name, add_time, update_time) "
            "ON target.name = source.name "
            "WHEN MATCHED THEN UPDATE SET "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(name, AddTime, UpdateTime) "
            "VALUES (source.name, source.add_time, source.update_time);"
        )

    def _build_row(self, record: SportRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.name,
            add_time,
            update_time,
        )


__all__ = ["SportRepository"]
