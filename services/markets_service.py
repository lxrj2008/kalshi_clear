"""Helpers for retrieving Kalshi markets with pagination support."""
from __future__ import annotations

import json
import logging
from typing import Any, Optional, Tuple

from http_request_demo import fetch_markets
from kalshi_client import KalshiAPIClient
from models.market_record import MarketRecord


class MarketsService:
    """Encapsulate interactions with market-related endpoints."""

    def __init__(self, client: KalshiAPIClient, logger: Optional[logging.Logger] = None) -> None:
        self._client = client
        self._logger = (logger or logging.getLogger("kalshi")).getChild(
            self.__class__.__name__.lower()
        )

    def list_market_records(self, **filters: Any) -> Tuple[list[MarketRecord], Optional[str]]:
        params = self._build_params(**filters)
        payload = self._fetch_raw_markets(params)
        markets = payload.get("markets", []) if isinstance(payload, dict) else []
        cursor = payload.get("cursor") if isinstance(payload, dict) else None
        records = [MarketRecord.from_api(market) for market in markets]
        return records, cursor

    def _build_params(self, **filters: Any) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if (limit := filters.get("limit")) is not None:
            params["limit"] = limit
        if (cursor := filters.get("cursor")) is not None:
            params["cursor"] = cursor
        if (event_ticker := filters.get("event_ticker")):
            params["event_ticker"] = event_ticker
        if (series_ticker := filters.get("series_ticker")):
            params["series_ticker"] = series_ticker
        if(min_created_ts := filters.get("min_created_ts")) is not None:
            params["min_created_ts"] = min_created_ts
        if(max_created_ts := filters.get("max_created_ts")) is not None:
            params["max_created_ts"] = max_created_ts
        if(min_updated_ts := filters.get("min_updated_ts")) is not None:
            params["min_updated_ts"] = min_updated_ts
        if (max_close_ts := filters.get("max_close_ts")) is not None:
            params["max_close_ts"] = max_close_ts
        if (min_close_ts := filters.get("min_close_ts")) is not None:
            params["min_close_ts"] = min_close_ts
        if(min_settled_ts := filters.get("min_settled_ts")) is not None:
            params["min_settled_ts"] = min_settled_ts
        if(max_settled_ts := filters.get("max_settled_ts")) is not None:
            params["max_settled_ts"] = max_settled_ts
        if (status := filters.get("status")):
            params["status"] = status
        if (tickers := filters.get("tickers")):
            params["tickers"] = tickers
        return params

    def _fetch_raw_markets(self, params: dict[str, Any]) -> dict[str, Any]:
        payload = fetch_markets(settings=self._client.settings, logger=self._logger, **params)
        if isinstance(payload, dict):
            return payload
        self._logger.error("Unexpected market payload type: %s", type(payload))
        return {}
        # response = self._client.call(
        #     "get_markets_without_preload_content", authenticated=True, **params
        # )
        # raw = getattr(response, "data", None)
        # if isinstance(raw, (bytes, bytearray)):
        #     text = raw.decode("utf-8")
        # else:
        #     text = str(raw or "")
        # try:
        #     return json.loads(text)
        # except json.JSONDecodeError:
        #     self._logger.error("Unable to decode market payload: %s", text[:200])
        #     return {}


__all__ = ["MarketsService", "MarketRecord"]
