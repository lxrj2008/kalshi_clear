"""Application-specific helpers around series-related Kalshi endpoints."""
from __future__ import annotations

from typing import Any, Optional

from kalshi_client import KalshiAPIClient, KalshiAPIError, AuthenticationConfigError


class SeriesService:
    """Encapsulate series queries so other code stays focused on business logic."""

    def __init__(self, client: KalshiAPIClient):
        self._client = client

    def list_series(self, status: Optional[str] = None) -> Any:
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        return self._client.call("get_series", authenticated=True, **params)


__all__ = [
    "SeriesService",
    "KalshiAPIError",
    "AuthenticationConfigError",
]
