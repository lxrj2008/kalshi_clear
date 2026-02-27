"""Normalized representation of Kalshi events for downstream processing."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional


def _get_value(source: Any, attribute: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(attribute)
    return getattr(source, attribute, None)


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


@dataclass(frozen=True)
class EventRecord:
    event_ticker: str
    series_ticker: Optional[str]
    category: Optional[str]
    title: Optional[str]
    sub_title: Optional[str]
    available_on_brokers: Optional[bool]
    collateral_return_type: Optional[str]
    mutually_exclusive: Optional[bool]
    strike_date: Optional[datetime]
    strike_period: Optional[str]
    last_updated_ts: Optional[datetime]
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_api(cls, item: Any) -> "EventRecord":
        return cls(
            event_ticker=str(_get_value(item, "event_ticker") or ""),
            series_ticker=_get_value(item, "series_ticker"),
            category=_get_value(item, "category"),
            title=_get_value(item, "title"),
            sub_title=_get_value(item, "sub_title"),
            available_on_brokers=_as_bool(_get_value(item, "available_on_brokers")),
            collateral_return_type=_get_value(item, "collateral_return_type"),
            mutually_exclusive=_as_bool(_get_value(item, "mutually_exclusive")),
            strike_date=_parse_datetime(_get_value(item, "strike_date")),
            strike_period=_get_value(item, "strike_period"),
            last_updated_ts=_parse_datetime(_get_value(item, "last_updated_ts")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = ["EventRecord"]
