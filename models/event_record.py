"""Normalized representation of Kalshi events for downstream processing."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, List, Optional

from kalshi_python.models.event import Event


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
    def from_api(cls, item: Event) -> "EventRecord":
        return cls(
            event_ticker=item.event_ticker,
            series_ticker=item.series_ticker,
            sub_title=item.sub_title,
            title=item.title,
            status=item.status,
            markets=item.markets,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = ["EventRecord"]
