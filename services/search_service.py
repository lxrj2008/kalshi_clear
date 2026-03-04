"""Search-related Kalshi helpers using the shared HTTP client."""
from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Optional

from kalshi_client import KalshiAPIClient


class SearchService:
    """Encapsulate search endpoints (tags, filters) behind the API client."""

    def __init__(self, client: KalshiAPIClient, logger: Optional[logging.Logger] = None) -> None:
        self._client = client
        self._logger = (logger or logging.getLogger("kalshi")).getChild(
            self.__class__.__name__.lower()
        )

    def fetch_tags_by_categories(self) -> dict[str, list[str] | None]:
        response = self._client.http_request(
            "GET",
            "/search/tags_by_categories",
            authenticated=True,
        )
        try:
            payload = response.json()
        except ValueError:
            self._logger.error(
                "Unable to parse tags_by_categories JSON; status=%s",
                response.status_code,
            )
            return {}

        if not isinstance(payload, Mapping):
            self._logger.error("Unexpected tags_by_categories payload type: %s", type(payload))
            return {}

        categories = payload.get("tags_by_categories")
        if not isinstance(categories, Mapping):
            return {}

        normalized: dict[str, list[str] | None] = {}
        for key, value in categories.items():
            if key is None:
                continue
            cat_name = str(key).strip()
            if not cat_name:
                continue
            if isinstance(value, list):
                normalized[cat_name] = [str(tag).strip() for tag in value if str(tag).strip()]
            else:
                normalized[cat_name] = None
        return normalized

    def fetch_filters_by_sport(self) -> dict[str, dict[str, object]]:
        response = self._client.http_request(
            "GET",
            "/search/filters_by_sport",
            authenticated=True,
        )
        try:
            payload = response.json()
        except ValueError:
            self._logger.error(
                "Unable to parse filters_by_sport JSON; status=%s",
                response.status_code,
            )
            return {}

        if not isinstance(payload, Mapping):
            self._logger.error("Unexpected filters_by_sport payload type: %s", type(payload))
            return {}

        filters = payload.get("filters_by_sports")
        if not isinstance(filters, Mapping):
            return {}

        normalized: dict[str, dict[str, object]] = {}
        for sport_key, sport_payload in filters.items():
            if sport_key is None:
                continue
            sport_name = str(sport_key).strip()
            if not sport_name:
                continue
            scopes: list[str] = []
            competitions: dict[str, list[str]] = {}

            if isinstance(sport_payload, Mapping):
                raw_scopes = sport_payload.get("scopes")
                if isinstance(raw_scopes, list):
                    scopes = [str(scope).strip() for scope in raw_scopes if str(scope).strip()]

                raw_competitions = sport_payload.get("competitions")
                if isinstance(raw_competitions, Mapping):
                    for comp_key, comp_payload in raw_competitions.items():
                        if comp_key is None:
                            continue
                        comp_name = str(comp_key).strip()
                        if not comp_name:
                            continue
                        comp_scopes: list[str] = []
                        if isinstance(comp_payload, Mapping):
                            comp_raw_scopes = comp_payload.get("scopes")
                            if isinstance(comp_raw_scopes, list):
                                comp_scopes = [
                                    str(scope).strip()
                                    for scope in comp_raw_scopes
                                    if str(scope).strip()
                                ]
                        competitions[comp_name] = comp_scopes

            normalized[sport_name] = {
                "scopes": scopes,
                "competitions": competitions,
            }

        return normalized


__all__ = ["SearchService"]
