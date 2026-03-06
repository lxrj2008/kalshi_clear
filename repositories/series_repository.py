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
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (" + ", ".join(["?"] * 13) + ")) AS source "
            "(ticker, category, contract_terms_url, contract_url, fee_multiplier, fee_type, frequency, last_updated_ts, title, volume, volume_fp, add_time, update_time) "
            "ON target.ticker = source.ticker "
            "WHEN MATCHED THEN UPDATE SET "
            "category = source.category, "
            "contract_terms_url = source.contract_terms_url, "
            "contract_url = source.contract_url, "
            "fee_multiplier = source.fee_multiplier, "
            "fee_type = source.fee_type, "
            "frequency = source.frequency, "
            "last_updated_ts = source.last_updated_ts, "
            "title = source.title, "
            "volume = source.volume, "
            "volume_fp = source.volume_fp, "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(ticker, category, contract_terms_url, contract_url, fee_multiplier, fee_type, frequency, last_updated_ts, title, volume, volume_fp, AddTime, UpdateTime) "
            "VALUES (source.ticker, source.category, source.contract_terms_url, source.contract_url, source.fee_multiplier, source.fee_type, source.frequency, source.last_updated_ts, source.title, source.volume, source.volume_fp, source.add_time, source.update_time);"
        )

    def _build_row(self, record: SeriesRecord) -> tuple[object, ...]:
        current_time = datetime.now()
        add_time = record.add_time or current_time
        update_time = record.update_time or current_time
        return (
            record.ticker,
            record.category,
            record.contract_terms_url,
            record.contract_url,
            record.fee_multiplier,
            record.fee_type,
            record.frequency,
            record.last_updated_ts,
            record.title,
            record.volume,
            record.volume_fp,
            add_time,
            update_time,
        )


__all__ = ["SeriesRepository"]
