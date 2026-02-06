"""Helpers for retrieving Kalshi events through the reusable API client."""
from __future__ import annotations

from typing import Any, Optional, Tuple

from kalshi_client import KalshiAPIClient
from models.event_record import EventRecord


class EventsService:
    """Encapsulate interactions with event-related Kalshi endpoints."""

    def __init__(self, client: KalshiAPIClient) -> None:
        self._client = client

    def list_events(
        self,
        *,
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
        with_nested_markets: Optional[bool] = None,
        with_milestones: Optional[bool] = None,
        status: Optional[str] = None,
        series_ticker: Optional[str] = None,
        min_close_ts: Optional[int] = None,
    ) -> Any:
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if cursor is not None:
            params["cursor"] = cursor
        if with_nested_markets is not None:
            params["with_nested_markets"] = with_nested_markets
        if with_milestones is not None:
            params["with_milestones"] = with_milestones
        if status:
            params["status"] = status
        if series_ticker:
            params["series_ticker"] = series_ticker
        if min_close_ts is not None:
            params["min_close_ts"] = min_close_ts
        return self._client.call("get_events", authenticated=True, **params)

    def list_event_records(
        self,
        **filters: Any,
    ) -> Tuple[list[EventRecord], list[Any], Optional[str]]:
        response = self.list_events(**filters)
        events = getattr(response, "events", None) or []
        milestones = getattr(response, "milestones", None) or []
        cursor = getattr(response, "cursor", None)
        records = [EventRecord.from_api(event) for event in events]
        return records, milestones, cursor


__all__ = ["EventsService", "EventRecord"]
