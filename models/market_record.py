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
    series_ticker: Optional[str]
    event_ticker: Optional[str]
    title: Optional[str]
    sub_title: Optional[str]
    status: Optional[str]
    open_time: Optional[datetime]
    close_time: Optional[datetime]
    expiration_time: Optional[datetime]
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    last_price: Optional[float]
    volume: Optional[int]
    volume_24h: Optional[int]
    result: Optional[str]
    can_close_early: Optional[bool]
    cap_count: Optional[int]
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_api(cls, item: Any) -> "MarketRecord":
        return cls(
            ticker=str(_get_value(item, "ticker") or ""),
            series_ticker=_get_value(item, "series_ticker"),
            event_ticker=_get_value(item, "event_ticker"),
            title=_get_value(item, "title"),
            sub_title=_get_value(item, "subtitle") or _get_value(item, "sub_title"),
            status=_get_value(item, "status"),
            open_time=_parse_datetime(_get_value(item, "open_time")),
            close_time=_parse_datetime(_get_value(item, "close_time")),
            expiration_time=_parse_datetime(_get_value(item, "expiration_time")),
            yes_bid=_as_float(_get_value(item, "yes_bid")),
            yes_ask=_as_float(_get_value(item, "yes_ask")),
            no_bid=_as_float(_get_value(item, "no_bid")),
            no_ask=_as_float(_get_value(item, "no_ask")),
            last_price=_as_float(_get_value(item, "last_price")),
            volume=_as_int(_get_value(item, "volume")),
            volume_24h=_as_int(_get_value(item, "volume_24h")),
            result=_get_value(item, "result"),
            can_close_early=_get_value(item, "can_close_early"),
            cap_count=_as_int(_get_value(item, "cap_count")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = ["MarketRecord"]
