"""Lightweight structures that make Kalshi series data easy to persist."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _get_value(source: Any, attribute: str) -> Any:
    if isinstance(source, Mapping):
        return source.get(attribute)
    return getattr(source, attribute, None)


@dataclass(frozen=True)
class SeriesRecord:
    """Normalized view of a Kalshi series suitable for persistence."""

    ticker: str
    category: Optional[str]
    contract_terms_url: Optional[str]
    contract_url: Optional[str]
    fee_multiplier: Optional[float]
    fee_type: Optional[str]
    frequency: Optional[str]
    last_updated_ts: Optional[datetime]
    title: Optional[str]
    volume: Optional[int]
    volume_fp: Optional[float]
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_api(cls, item: Any) -> "SeriesRecord":
        return cls(
            ticker=str(_get_value(item, "ticker") or ""),
            category=_get_value(item, "category"),
            contract_terms_url=_get_value(item, "contract_terms_url"),
            contract_url=_get_value(item, "contract_url"),
            fee_multiplier=_as_float(_get_value(item, "fee_multiplier")),
            fee_type=_get_value(item, "fee_type"),
            frequency=_get_value(item, "frequency"),
            last_updated_ts=_parse_datetime(_get_value(item, "last_updated_ts")),
            title=_get_value(item, "title"),
            volume=_as_int(_get_value(item, "volume")),
            volume_fp=_as_float(_get_value(item, "volume_fp")),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict (e.g., for parameterized SQL inserts)."""
        return asdict(self)

__all__ = ["SeriesRecord"]
