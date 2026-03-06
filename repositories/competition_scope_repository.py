"""SQL Server persistence for competition-to-scope mappings."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.competition_scope_record import CompetitionScopeRecord
from repositories.base_repository import BaseSQLRepository


class CompetitionScopeRepository(BaseSQLRepository):
    """Insert or update competition/scope relationships in SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Competition_Scopes",
        database_name: str | None = None,
    ) -> None:
        super().__init__(settings, logger=logger, database_name=database_name)
        self.table_name = table_name

    def save_competition_scopes(self, records: Sequence[CompetitionScopeRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (?, ?, ?, ?)) AS source "
            "(competition_name, scope_name, add_time, update_time) "
            "ON target.competition_name = source.competition_name AND target.scope_name = source.scope_name "
            "WHEN MATCHED THEN UPDATE SET "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(competition_name, scope_name, AddTime, UpdateTime) "
            "VALUES (source.competition_name, source.scope_name, source.add_time, source.update_time);"
        )

    def _build_row(self, record: CompetitionScopeRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.competition_name,
            record.scope_name,
            add_time,
            update_time,
        )


__all__ = ["CompetitionScopeRepository"]
