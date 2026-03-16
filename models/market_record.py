"""Normalized representation of Kalshi markets for persistence."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _get_value(source: Any, attribute: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(attribute)
    return getattr(source, attribute, None)


@dataclass(frozen=True)
class MarketRecord:
    ticker: str
    event_ticker: Optional[str]
    series_ticker: Optional[str]
    market_type: Optional[str]
    title: Optional[str]
    subtitle: Optional[str]
    yes_sub_title: Optional[str]
    no_sub_title: Optional[str]
    created_time: Optional[datetime]
    updated_time: Optional[datetime]
    open_time: Optional[datetime]
    close_time: Optional[datetime]
    expiration_time: Optional[datetime]
    latest_expiration_time: Optional[datetime]
    settlement_timer_seconds: Optional[int]
    status: Optional[str]
    response_price_units: Optional[str]
    yes_bid_dollars: Optional[float]
    yes_bid_size_fp: Optional[float]
    yes_ask_dollars: Optional[float]
    yes_ask_size_fp: Optional[float]
    no_bid_dollars: Optional[float]
    no_ask_dollars: Optional[float]
    last_price_dollars: Optional[float]
    volume_fp: Optional[float]
    volume_24h_fp: Optional[float]
    result: Optional[str]
    can_close_early: Optional[bool]
    fractional_trading_enabled: Optional[bool]
    open_interest_fp: Optional[float]
    notional_value_dollars: Optional[float]
    previous_yes_bid_dollars: Optional[float]
    previous_yes_ask_dollars: Optional[float]
    previous_price_dollars: Optional[float]
    liquidity_dollars: Optional[float]
    expiration_value: Optional[str]
    tick_size: Optional[int]
    rules_primary: Optional[str]
    rules_secondary: Optional[str]
    price_level_structure: Optional[str]
    expected_expiration_time: Optional[datetime]
    settlement_value: Optional[float]
    settlement_value_dollars: Optional[float]
    settlement_ts: Optional[datetime]
    fee_waiver_expiration_time: Optional[datetime]
    early_close_condition: Optional[str]
    strike_type: Optional[str]
    floor_strike: Optional[float]
    cap_strike: Optional[float]
    functional_strike: Optional[str]
    mve_collection_ticker: Optional[str]
    primary_participant_key: Optional[str]
    is_provisional: Optional[bool]
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_api(cls, item: Any) -> "MarketRecord":
        return cls(
            ticker=str(_get_value(item, "ticker") or ""),
            event_ticker=_get_value(item, "event_ticker"),
            series_ticker=_get_value(item, "series_ticker"),
            market_type=_get_value(item, "market_type"),
            title=_get_value(item, "title"),
            subtitle=_get_value(item, "subtitle") or _get_value(item, "sub_title"),
            yes_sub_title=_get_value(item, "yes_sub_title"),
            no_sub_title=_get_value(item, "no_sub_title"),
            created_time=_parse_datetime(_get_value(item, "created_time")),
            updated_time=_parse_datetime(_get_value(item, "updated_time")),
            open_time=_parse_datetime(_get_value(item, "open_time")),
            close_time=_parse_datetime(_get_value(item, "close_time")),
            expiration_time=_parse_datetime(_get_value(item, "expiration_time")),
            latest_expiration_time=_parse_datetime(
                _get_value(item, "latest_expiration_time")
            ),
            settlement_timer_seconds=_as_int(_get_value(item, "settlement_timer_seconds")),
            status=_get_value(item, "status"),
            response_price_units=_get_value(item, "response_price_units"),
            yes_bid_dollars=_as_float(_get_value(item, "yes_bid_dollars")),
            yes_bid_size_fp=_as_float(_get_value(item, "yes_bid_size_fp")),
            yes_ask_dollars=_as_float(_get_value(item, "yes_ask_dollars")),
            yes_ask_size_fp=_as_float(_get_value(item, "yes_ask_size_fp")),
            no_bid_dollars=_as_float(_get_value(item, "no_bid_dollars")),
            no_ask_dollars=_as_float(_get_value(item, "no_ask_dollars")),
            last_price_dollars=_as_float(_get_value(item, "last_price_dollars")),
            volume_fp=_as_float(_get_value(item, "volume_fp")),
            volume_24h_fp=_as_float(_get_value(item, "volume_24h_fp")),
            result=_get_value(item, "result"),
            can_close_early=_as_bool(_get_value(item, "can_close_early")),
            fractional_trading_enabled=_as_bool(
                _get_value(item, "fractional_trading_enabled")
            ),
            open_interest_fp=_as_float(_get_value(item, "open_interest_fp")),
            notional_value_dollars=_as_float(_get_value(item, "notional_value_dollars")),
            previous_yes_bid_dollars=_as_float(
                _get_value(item, "previous_yes_bid_dollars")
            ),
            previous_yes_ask_dollars=_as_float(
                _get_value(item, "previous_yes_ask_dollars")
            ),
            previous_price_dollars=_as_float(
                _get_value(item, "previous_price_dollars")
            ),
            liquidity_dollars=_as_float(_get_value(item, "liquidity_dollars")),
            expiration_value=_get_value(item, "expiration_value"),
            tick_size=_as_int(_get_value(item, "tick_size")),
            rules_primary=_get_value(item, "rules_primary"),
            rules_secondary=_get_value(item, "rules_secondary"),
            price_level_structure=_get_value(item, "price_level_structure"),
            expected_expiration_time=_parse_datetime(
                _get_value(item, "expected_expiration_time")
            ),
            settlement_value=_as_float(_get_value(item, "settlement_value")),
            settlement_value_dollars=_as_float(
                _get_value(item, "settlement_value_dollars")
            ),
            settlement_ts=_parse_datetime(_get_value(item, "settlement_ts")),
            fee_waiver_expiration_time=_parse_datetime(
                _get_value(item, "fee_waiver_expiration_time")
            ),
            early_close_condition=_get_value(item, "early_close_condition"),
            strike_type=_get_value(item, "strike_type"),
            floor_strike=_as_float(_get_value(item, "floor_strike")),
            cap_strike=_as_float(_get_value(item, "cap_strike")),
            functional_strike=_get_value(item, "functional_strike"),
            mve_collection_ticker=_get_value(item, "mve_collection_ticker"),
            primary_participant_key=_get_value(item, "primary_participant_key"),
            is_provisional=_as_bool(_get_value(item, "is_provisional")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = ["MarketRecord"]
