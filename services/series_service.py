"""Application-specific helpers around series-related Kalshi endpoints."""
from __future__ import annotations

from typing import Any, Optional

from kalshi_client import KalshiAPIClient, KalshiAPIError, AuthenticationConfigError
from models.series_record import SeriesRecord


class SeriesService:
    """Encapsulate series queries so other code stays focused on business logic."""

    def __init__(self, client: KalshiAPIClient):
        self._client = client

    def list_series(self, status: Optional[str] = None) -> Any:
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        return self._client.call("get_series", authenticated=True, **params)

    def list_series_records(self, status: Optional[str] = None) -> list[SeriesRecord]:
        response = self.list_series(status=status)
        items = getattr(response, "series", None) or []
        return [SeriesRecord.from_api(item) for item in items]


__all__ = [
    "SeriesService",
    "KalshiAPIError",
    "AuthenticationConfigError",
    "SeriesRecord",
]
