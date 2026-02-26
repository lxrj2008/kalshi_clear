"""Normalized representation of sports for persistence."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class SportRecord:
    name: str
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = ["SportRecord"]
