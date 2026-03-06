"""Helpers for retrieving Kalshi events through the reusable API client."""
from __future__ import annotations

import logging
from typing import Any, Optional, Tuple

from kalshi_client import KalshiAPIClient
from models.event_record import EventRecord


class EventsService:
    """Encapsulate interactions with event-related Kalshi endpoints."""

    def __init__(self, client: KalshiAPIClient, logger: Optional[logging.Logger] = None) -> None:
        self._client = client
        self._logger = (logger or logging.getLogger("kalshi")).getChild(
            self.__class__.__name__.lower()
        )

    def list_event_records(self,**filters: Any,) -> Tuple[list[EventRecord], list[Any], Optional[str]]:
        params = self._build_params(**filters)
        payload = self._fetch_raw_events(params)
        events = payload.get("events", []) if isinstance(payload, dict) else []
        milestones = payload.get("milestones", []) if isinstance(payload, dict) else []
        cursor = payload.get("cursor") if isinstance(payload, dict) else None
        records = [EventRecord.from_api(event) for event in events]
        return records, milestones, cursor

    def fetch_event_record(self, event_ticker: str) -> Optional[EventRecord]:
        payload = self._fetch_raw_event(event_ticker)
        event = payload.get("event") if isinstance(payload, dict) else None
        if not event:
            self._logger.warning("No event payload returned for event_ticker=%s", event_ticker)
            return None
        return EventRecord.from_api(event)

    def _build_params(self, **filters: Any) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if (limit := filters.get("limit")) is not None:
            params["limit"] = limit
        if (cursor := filters.get("cursor")) is not None:
            params["cursor"] = cursor
        if (with_nested_markets := filters.get("with_nested_markets")) is not None:
            params["with_nested_markets"] = with_nested_markets
        if (with_milestones := filters.get("with_milestones")) is not None:
            params["with_milestones"] = with_milestones
        if (status := filters.get("status")):
            params["status"] = status
        if (series_ticker := filters.get("series_ticker")):
            params["series_ticker"] = series_ticker
        if (min_close_ts := filters.get("min_close_ts")) is not None:
            params["min_close_ts"] = min_close_ts
        if(min_updated_ts := filters.get("min_updated_ts")) is not None:
            params["min_updated_ts"] = min_updated_ts
        return params

    def _fetch_raw_events(self, params: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self._client.http_request(
                "GET",
                "/events",
                authenticated=True,
                params=params,
            )
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            self._logger.error("Unexpected event payload type: %s", type(payload))
            return {}
        except Exception as exc:  
            self._logger.error("Unable to fetch events: %s", exc)
            return {}

    def _fetch_raw_event(self, event_ticker: str) -> dict[str, Any]:
        url = f"/events/{event_ticker}"
        try:
            response = self._client.http_request("GET", url, authenticated=True)
            return response.json()
        except Exception as exc:  
            self._logger.error("Unable to fetch event %s: %s", event_ticker, exc)
            return {}


__all__ = ["EventsService", "EventRecord"]
