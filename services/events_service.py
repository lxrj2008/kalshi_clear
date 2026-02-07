"""Helpers for retrieving Kalshi events through the reusable API client."""
from __future__ import annotations

import json
import logging
from typing import Any, Optional, Tuple

from pydantic import ValidationError

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
        try:
            response = self._client.call("get_events", authenticated=True, **params)
            events = getattr(response, "events", None) or []
            milestones = getattr(response, "milestones", None) or []
            cursor = getattr(response, "cursor", None)
        except ValidationError as err:
            self._logger.warning(
                "Event payload validation failed (%s); falling back to raw parsing",
                err,
            )
            payload = self._fetch_raw_events(params)
            events = payload.get("events", []) if isinstance(payload, dict) else []
            milestones = payload.get("milestones", []) if isinstance(payload, dict) else []
            cursor = payload.get("cursor") if isinstance(payload, dict) else None
        records = [EventRecord.from_api(event) for event in events]
        return records, milestones, cursor

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
        return params

    def _fetch_raw_events(self, params: dict[str, Any]) -> dict[str, Any]:
        response = self._client.call(
            "get_events_without_preload_content", authenticated=True, **params
        )
        raw = getattr(response, "data", None)
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw or "")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            self._logger.error("Unable to decode event payload: %s", text[:200])
            return {}


__all__ = ["EventsService", "EventRecord"]
