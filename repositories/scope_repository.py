"""SQL Server persistence for scopes (market types)."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.scope_record import ScopeRecord
from repositories.base_repository import BaseSQLRepository


class ScopeRepository(BaseSQLRepository):
    """Insert or update scopes in SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Scopes",
        database_name: str | None = None,
    ) -> None:
        super().__init__(settings, logger=logger, database_name=database_name)
        self.table_name = table_name

    def save_scopes(self, records: Sequence[ScopeRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  
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

    def _build_row(self, record: ScopeRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.name,
            add_time,
            update_time,
        )


__all__ = ["ScopeRepository"]
