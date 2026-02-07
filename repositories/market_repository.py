"""SQL Server persistence for Kalshi markets."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.market_record import MarketRecord
from repositories.base_repository import BaseSQLRepository


class MarketRepository(BaseSQLRepository):
    """Insert or update market snapshots in SQL Server."""

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Markets",
    ) -> None:
        super().__init__(settings, logger=logger)
        self.table_name = table_name

    def save_markets(self, records: Sequence[MarketRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        self.logger.debug("Prepared %s parameter sets for market upsert", len(rows))
        return self._executemany(self.insert_statement, rows)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source "
            "(ticker, event_ticker, series_ticker, title, sub_title, status, open_time, close_time, expiration_time, yes_bid, yes_ask, no_bid, no_ask, last_price, volume, volume_24h, result, can_close_early, cap_count, add_time, update_time) "
            "ON target.ticker = source.ticker "
            "WHEN MATCHED THEN UPDATE SET "
            "event_ticker = source.event_ticker, "
            "series_ticker = source.series_ticker, "
            "title = source.title, "
            "sub_title = source.sub_title, "
            "status = source.status, "
            "open_time = source.open_time, "
            "close_time = source.close_time, "
            "expiration_time = source.expiration_time, "
            "yes_bid = source.yes_bid, "
            "yes_ask = source.yes_ask, "
            "no_bid = source.no_bid, "
            "no_ask = source.no_ask, "
            "last_price = source.last_price, "
            "volume = source.volume, "
            "volume_24h = source.volume_24h, "
            "result = source.result, "
            "can_close_early = source.can_close_early, "
            "cap_count = source.cap_count, "
            "UpdateTime = source.update_time "
            "WHEN NOT MATCHED THEN INSERT "
            "(ticker, event_ticker, series_ticker, title, sub_title, status, open_time, close_time, expiration_time, yes_bid, yes_ask, no_bid, no_ask, last_price, volume, volume_24h, result, can_close_early, cap_count, AddTime, UpdateTime) "
            "VALUES (source.ticker, source.event_ticker, source.series_ticker, source.title, source.sub_title, source.status, source.open_time, source.close_time, source.expiration_time, source.yes_bid, source.yes_ask, source.no_bid, source.no_ask, source.last_price, source.volume, source.volume_24h, source.result, source.can_close_early, source.cap_count, source.add_time, source.update_time);"
        )

    def _build_row(self, record: MarketRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.ticker,
            record.event_ticker,
            record.series_ticker,
            record.title,
            record.sub_title,
            record.status,
            record.open_time,
            record.close_time,
            record.expiration_time,
            record.yes_bid,
            record.yes_ask,
            record.no_bid,
            record.no_ask,
            record.last_price,
            record.volume,
            record.volume_24h,
            record.result,
            record.can_close_early,
            record.cap_count,
            add_time,
            update_time,
        )


__all__ = ["MarketRepository"]
