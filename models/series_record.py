"""Lightweight structures that make Kalshi series data easy to persist."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Optional

from kalshi_python.models.series import Series


@dataclass(frozen=True)
class SeriesRecord:
    """Normalized view of a Kalshi series suitable for persistence."""

    ticker: str
    title: str
    category: str
    status: Optional[str]

    @classmethod
    def from_api(cls, item: Series) -> "SeriesRecord":
        """Build a record from the Kalshi SDK object."""
        return cls(
            ticker=item.ticker,
            title=item.title,
            category=(item.category or ""),
            status=item.status,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict (e.g., for parameterized SQL inserts)."""
        return asdict(self)

    def to_sql_params(self) -> tuple[Any, ...]:
        """Return values in a stable column order for executemany()."""
        return (self.ticker, self.title, self.category, self.status)


__all__ = ["SeriesRecord"]
