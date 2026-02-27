"""Application-specific helpers around series-related Kalshi endpoints."""
from __future__ import annotations

import logging
from typing import Any, Optional

from http_request_demo import fetch_series
from kalshi_client import KalshiAPIClient, KalshiAPIError, AuthenticationConfigError
from models.series_record import SeriesRecord


class SeriesService:
    """Encapsulate series queries so other code stays focused on business logic."""

    def __init__(self, client: KalshiAPIClient, logger: Optional[logging.Logger] = None):
        self._client = client
        self._logger = (logger or logging.getLogger("kalshi")).getChild(
            self.__class__.__name__.lower()
        )

    def list_series_records(self, **filters: Any) -> list[SeriesRecord]:
        params = self._build_params(**filters)
        payload = self._fetch_raw_series(params)
        items = payload.get("series", []) if isinstance(payload, dict) else []
        return [SeriesRecord.from_api(item) for item in items]

    def _build_params(self, **filters: Any) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if(category := filters.get("category")):
            params["category"] = category
        if(include_volume := filters.get("include_volume")) is not None:
            params["include_volume"] = include_volume
        if(tags := filters.get("tags")):
            params["tags"] = tags

        # Pass through any additional filters that were provided explicitly.
        for key, value in filters.items():
            if key in params or value is None:
                continue
            params[key] = value
        return params

    def _fetch_raw_series(self, params: dict[str, Any]) -> dict[str, Any]:
        payload = fetch_series(settings=self._client.settings, logger=self._logger, **params)
        if isinstance(payload, dict):
            return payload
        self._logger.error("Unexpected series payload type: %s", type(payload))
        return {}


__all__ = [
    "SeriesService",
    "KalshiAPIError",
    "AuthenticationConfigError",
    "SeriesRecord",
]
