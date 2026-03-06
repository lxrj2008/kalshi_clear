"""SQL Server persistence for sport-to-scope mappings."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.sport_scope_record import SportScopeRecord
from repositories.base_repository import BaseSQLRepository


class SportScopeRepository(BaseSQLRepository):
    """Insert or update sport/scope relationships in SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Sport_Scopes",
        database_name: str | None = None,
    ) -> None:
        super().__init__(settings, logger=logger, database_name=database_name)
        self.table_name = table_name

    def save_sport_scopes(self, records: Sequence[SportScopeRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (?, ?, ?, ?)) AS source "
            "(sport_name, scope_name, add_time, update_time) "
            "ON target.sport_name = source.sport_name AND target.scope_name = source.scope_name "
            "WHEN MATCHED THEN UPDATE SET "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(sport_name, scope_name, AddTime, UpdateTime) "
            "VALUES (source.sport_name, source.scope_name, source.add_time, source.update_time);"
        )

    def _build_row(self, record: SportScopeRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.sport_name,
            record.scope_name,
            add_time,
            update_time,
        )


__all__ = ["SportScopeRepository"]
