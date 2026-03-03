"""SQL Server persistence for Kalshi markets."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from config import KalshiSettings
from models.market_record import MarketRecord
from repositories.base_repository import BaseSQLRepository


class MarketRepository(BaseSQLRepository):
    """Insert market snapshots when absent in SQL Server."""

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
        self.logger.debug("Prepared %s parameter sets for market insert-if-absent", len(rows))
        return self._executemany(self.insert_statement, rows)

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
            self.logger.debug("No market fields supplied for update; ticker=%s", ticker)
            return 0

        params.append(ticker)
        statement = f"UPDATE {self.table_name} SET " + ", ".join(assignments) + " WHERE ticker = ?"
        return self._execute_update(statement, params)

    @property
    def insert_statement(self) -> str:  # type: ignore[override]
        return (
            f"MERGE {self.table_name} AS target "
            "USING (VALUES (" + ", ".join(["?"] * 68) + ")) AS source "
            "(ticker, event_ticker, series_ticker, market_type, title, subtitle, yes_sub_title, no_sub_title, created_time, updated_time, open_time, close_time, expiration_time, latest_expiration_time, settlement_timer_seconds, status, response_price_units, yes_bid, yes_bid_dollars, yes_bid_size_fp, yes_ask, yes_ask_dollars, yes_ask_size_fp, no_bid, no_bid_dollars, no_ask, no_ask_dollars, last_price, last_price_dollars, volume, volume_fp, volume_24h, volume_24h_fp, result, can_close_early, fractional_trading_enabled, open_interest, open_interest_fp, notional_value, notional_value_dollars, previous_yes_bid, previous_yes_bid_dollars, previous_yes_ask, previous_yes_ask_dollars, previous_price, previous_price_dollars, liquidity, liquidity_dollars, expiration_value, tick_size, rules_primary, rules_secondary, price_level_structure, expected_expiration_time, settlement_value, settlement_value_dollars, settlement_ts, fee_waiver_expiration_time, early_close_condition, strike_type, floor_strike, cap_strike, functional_strike, mve_collection_ticker, primary_participant_key, is_provisional, AddTime, UpdateTime) "
            "ON target.ticker = source.ticker "
            "WHEN NOT MATCHED THEN INSERT "
            "(ticker, event_ticker, series_ticker, market_type, title, subtitle, yes_sub_title, no_sub_title, created_time, updated_time, open_time, close_time, expiration_time, latest_expiration_time, settlement_timer_seconds, status, response_price_units, yes_bid, yes_bid_dollars, yes_bid_size_fp, yes_ask, yes_ask_dollars, yes_ask_size_fp, no_bid, no_bid_dollars, no_ask, no_ask_dollars, last_price, last_price_dollars, volume, volume_fp, volume_24h, volume_24h_fp, result, can_close_early, fractional_trading_enabled, open_interest, open_interest_fp, notional_value, notional_value_dollars, previous_yes_bid, previous_yes_bid_dollars, previous_yes_ask, previous_yes_ask_dollars, previous_price, previous_price_dollars, liquidity, liquidity_dollars, expiration_value, tick_size, rules_primary, rules_secondary, price_level_structure, expected_expiration_time, settlement_value, settlement_value_dollars, settlement_ts, fee_waiver_expiration_time, early_close_condition, strike_type, floor_strike, cap_strike, functional_strike, mve_collection_ticker, primary_participant_key, is_provisional, AddTime, UpdateTime) "
            "VALUES (source.ticker, source.event_ticker, source.series_ticker, source.market_type, source.title, source.subtitle, source.yes_sub_title, source.no_sub_title, source.created_time, source.updated_time, source.open_time, source.close_time, source.expiration_time, source.latest_expiration_time, source.settlement_timer_seconds, source.status, source.response_price_units, source.yes_bid, source.yes_bid_dollars, source.yes_bid_size_fp, source.yes_ask, source.yes_ask_dollars, source.yes_ask_size_fp, source.no_bid, source.no_bid_dollars, source.no_ask, source.no_ask_dollars, source.last_price, source.last_price_dollars, source.volume, source.volume_fp, source.volume_24h, source.volume_24h_fp, source.result, source.can_close_early, source.fractional_trading_enabled, source.open_interest, source.open_interest_fp, source.notional_value, source.notional_value_dollars, source.previous_yes_bid, source.previous_yes_bid_dollars, source.previous_yes_ask, source.previous_yes_ask_dollars, source.previous_price, source.previous_price_dollars, source.liquidity, source.liquidity_dollars, source.expiration_value, source.tick_size, source.rules_primary, source.rules_secondary, source.price_level_structure, source.expected_expiration_time, source.settlement_value, source.settlement_value_dollars, source.settlement_ts, source.fee_waiver_expiration_time, source.early_close_condition, source.strike_type, source.floor_strike, source.cap_strike, source.functional_strike, source.mve_collection_ticker, source.primary_participant_key, source.is_provisional, source.AddTime, source.UpdateTime);"
        )

    def _build_row(self, record: MarketRecord) -> tuple[object, ...]:
        # ensure timestamps for auditing if not supplied
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
            record.yes_bid,
            record.yes_bid_dollars,
            record.yes_bid_size_fp,
            record.yes_ask,
            record.yes_ask_dollars,
            record.yes_ask_size_fp,
            record.no_bid,
            record.no_bid_dollars,
            record.no_ask,
            record.no_ask_dollars,
            record.last_price,
            record.last_price_dollars,
            record.volume,
            record.volume_fp,
            record.volume_24h,
            record.volume_24h_fp,
            record.result,
            record.can_close_early,
            record.fractional_trading_enabled,
            record.open_interest,
            record.open_interest_fp,
            record.notional_value,
            record.notional_value_dollars,
            record.previous_yes_bid,
            record.previous_yes_bid_dollars,
            record.previous_yes_ask,
            record.previous_yes_ask_dollars,
            record.previous_price,
            record.previous_price_dollars,
            record.liquidity,
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
