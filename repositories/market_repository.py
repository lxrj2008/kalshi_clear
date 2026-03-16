"""SQL Server persistence for Kalshi markets."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.market_record import MarketRecord
from repositories.base_repository import BaseSQLRepository, DatabaseSaveError


class MarketRepository(BaseSQLRepository):
    """Insert market snapshots when absent in SQL Server."""

    # Keep column order aligned with _build_row tuples and SQL statements.
    COLUMNS: list[str] = [
        "ticker",
        "event_ticker",
        "series_ticker",
        "market_type",
        "title",
        "subtitle",
        "yes_sub_title",
        "no_sub_title",
        "created_time",
        "updated_time",
        "open_time",
        "close_time",
        "expiration_time",
        "latest_expiration_time",
        "settlement_timer_seconds",
        "status",
        "response_price_units",
        "yes_bid_dollars",
        "yes_bid_size_fp",
        "yes_ask_dollars",
        "yes_ask_size_fp",
        "no_bid_dollars",
        "no_ask_dollars",
        "last_price_dollars",
        "volume_fp",
        "volume_24h_fp",
        "result",
        "can_close_early",
        "fractional_trading_enabled",
        "open_interest_fp",
        "notional_value_dollars",
        "previous_yes_bid_dollars",
        "previous_yes_ask_dollars",
        "previous_price_dollars",
        "liquidity_dollars",
        "expiration_value",
        "tick_size",
        "rules_primary",
        "rules_secondary",
        "price_level_structure",
        "expected_expiration_time",
        "settlement_value",
        "settlement_value_dollars",
        "settlement_ts",
        "fee_waiver_expiration_time",
        "early_close_condition",
        "strike_type",
        "floor_strike",
        "cap_strike",
        "functional_strike",
        "mve_collection_ticker",
        "primary_participant_key",
        "is_provisional",
        "AddTime",
        "UpdateTime",
    ]

    def __init__(
        self,
        settings: KalshiSettings,
        logger: Optional[object] = None,
        table_name: str = "dbo.KS_Markets",
        staging_table: str = "dbo.KS_Markets_TEMP",
    ) -> None:
        super().__init__(settings, logger=logger)
        self.table_name = table_name
        self.staging_table = staging_table

    def save_markets(self, records: Sequence[MarketRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        batch_size = 10000
        for index in range(0, len(rows), batch_size):
            batch = rows[index : index + batch_size]
            self._executemany(self._staging_insert_statement, batch)
        affected = self._merge_from_staging(total=len(rows))
        self._truncate_staging()
        return affected

    def save_markets_direct(self, records: Sequence[MarketRecord]) -> int:
        rows = [self._build_row(record) for record in records]
        if not rows:
            return 0
        return self._executemany(self.insert_statement, rows)

    def reset_staging(self) -> None:
        """Explicitly truncate the staging table; callers can control boundaries."""
        self._truncate_staging()

    def update_market_fields(
        self,
        ticker: str,
        *,
        open_time: Optional[datetime] = None,
        close_time: Optional[datetime] = None,
        result: Optional[str] = None,
        settlement_value: Optional[float] = None,
        settlement_ts: Optional[datetime] = None,
        updated_time: Optional[datetime] = None,
        status: Optional[str] = None,
    ) -> int:
        assignments: list[str] = []
        params: list[object] = []
        if open_time is not None:
            assignments.append("open_time = ?")
            params.append(open_time)
        if close_time is not None:
            assignments.append("close_time = ?")
            params.append(close_time)
        if result is not None:
            assignments.append("result = ?")
            params.append(result)
        if settlement_value is not None:
            assignments.append("settlement_value = ?")
            params.append(settlement_value)
        if settlement_ts is not None:
            assignments.append("settlement_ts = ?")
            params.append(settlement_ts)
        if updated_time is not None:
            assignments.append("updated_time = ?")
            params.append(updated_time)
        if status is not None:
            assignments.append("status = ?")
            params.append(status)

        assignments.append("UpdateTime = ?")
        params.append(datetime.now())

        if not assignments:
            return 0

        params.append(ticker)
        statement = f"UPDATE {self.table_name} SET " + ", ".join(assignments) + " WHERE ticker = ?"
        return self._execute_update(statement, params)

    @property
    def insert_statement(self) -> str:  
        columns = ", ".join(self.COLUMNS)
        placeholders = ", ".join(["?"] * len(self.COLUMNS))
        source_values = ", ".join([f"source.{name}" for name in self.COLUMNS])
        return (
            f"MERGE {self.table_name} AS target "
            f"USING (VALUES ({placeholders})) AS source ({columns}) "
            "ON target.ticker = source.ticker "
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
            f"USING {self.staging_table} AS source ON target.ticker = source.ticker "
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
            raise DatabaseSaveError("Unable to persist rows to SQL Server") from exc
        affected = rowcount if rowcount >= 0 else total
        self.logger.info("Merged %s market rows from staging", affected)
        return affected

    def _build_row(self, record: MarketRecord) -> tuple[object, ...]:
        now = datetime.now()
        add_time = record.add_time or now
        update_time = record.update_time or now
        return (
            record.ticker,
            record.event_ticker,
            record.series_ticker,
            record.market_type,
            record.title,
            record.subtitle,
            record.yes_sub_title,
            record.no_sub_title,
            record.created_time,
            record.updated_time,
            record.open_time,
            record.close_time,
            record.expiration_time,
            record.latest_expiration_time,
            record.settlement_timer_seconds,
            record.status,
            record.response_price_units,
            record.yes_bid_dollars,
            record.yes_bid_size_fp,
            record.yes_ask_dollars,
            record.yes_ask_size_fp,
            record.no_bid_dollars,
            record.no_ask_dollars,
            record.last_price_dollars,
            record.volume_fp,
            record.volume_24h_fp,
            record.result,
            record.can_close_early,
            record.fractional_trading_enabled,
            record.open_interest_fp,
            record.notional_value_dollars,
            record.previous_yes_bid_dollars,
            record.previous_yes_ask_dollars,
            record.previous_price_dollars,
            record.liquidity_dollars,
            record.expiration_value,
            record.tick_size,
            record.rules_primary,
            record.rules_secondary,
            record.price_level_structure,
            record.expected_expiration_time,
            record.settlement_value,
            record.settlement_value_dollars,
            record.settlement_ts,
            record.fee_waiver_expiration_time,
            record.early_close_condition,
            record.strike_type,
            record.floor_strike,
            record.cap_strike,
            record.functional_strike,
            record.mve_collection_ticker,
            record.primary_participant_key,
            record.is_provisional,
            add_time,
            update_time,
        )


__all__ = ["MarketRepository"]
