"""Normalized representation of Kalshi events for downstream processing."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, List, Optional


def _get_value(source: Any, attribute: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(attribute)
    return getattr(source, attribute, None)


@dataclass(frozen=True)
class EventRecord:
    event_ticker: str
    series_ticker: Optional[str]
    sub_title: Optional[str]
    title: Optional[str]
    status: Optional[str]
    markets: Optional[List[Any]]
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_api(cls, item: Any) -> "EventRecord":
        return cls(
            event_ticker=str(_get_value(item, "event_ticker") or ""),
            series_ticker=_get_value(item, "series_ticker"),
            sub_title=_get_value(item, "sub_title"),
            title=_get_value(item, "title"),
            status=_get_value(item, "status"),
            markets=_get_value(item, "markets"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = ["EventRecord"]
