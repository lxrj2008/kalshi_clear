"""SQL Server persistence for tags grouped by category."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.tag_record import TagRecord
from repositories.base_repository import BaseSQLRepository


class TagRepository(BaseSQLRepository):
    """Insert or update tags in SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Tags",
    ) -> None:
        super().__init__(settings, logger=logger)
        self.table_name = table_name

    def save_tags(self, records: Sequence[TagRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        self.logger.debug("Prepared %s parameter sets for tag upsert", len(rows))
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (?, ?, ?, ?)) AS source "
            "(category, tag, add_time, update_time) "
            "ON target.category = source.category AND target.tag = source.tag "
            "WHEN MATCHED THEN UPDATE SET "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(category, tag, AddTime, UpdateTime) "
            "VALUES (source.category, source.tag, source.add_time, source.update_time);"
        )

    def _build_row(self, record: TagRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.category,
            record.tag,
            add_time,
            update_time,
        )


__all__ = ["TagRepository"]
