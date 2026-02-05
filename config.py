"""Application-wide configuration helpers for Kalshi API access."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KalshiSettings(BaseSettings):
    """Load configuration from environment variables or a .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    host: str = Field(
        "https://api.elections.kalshi.com/trade-api/v2",
        validation_alias="KALSHI_HOST",
        description="Kalshi API base URL",
    )
    api_key_id: Optional[str] = Field(
        None,
        validation_alias="KALSHI_API_KEY_ID",
        description="Kalshi API key identifier",
    )
    private_key_path: Optional[Path] = Field(
        None,
        validation_alias="KALSHI_PRIVATE_KEY_PATH",
        description="Filesystem path to the PEM formatted private key",
    )
    log_level: str = Field(
        "INFO",
        validation_alias="LOG_LEVEL",
        description="Root logging level (DEBUG, INFO, etc.)",
    )
    log_directory: Path = Field(
        Path("logs"),
        validation_alias="LOG_DIRECTORY",
        description="Directory where log files should be written",
    )

    @field_validator("private_key_path", mode="before")
    @classmethod
    def _expand_private_key_path(cls, value: Optional[Path]) -> Optional[Path]:
        if value in (None, ""):
            return None
        path = Path(value)
        return path.expanduser().resolve()

    @field_validator("log_directory", mode="before")
    @classmethod
    def _expand_log_directory(cls, value: Optional[Path]) -> Path:
        path = Path(value) if value not in (None, "") else Path("logs")
        return path.expanduser().resolve()

    def read_private_key(self) -> Optional[str]:
        """Return the PEM key content if a path was provided."""
        if self.private_key_path is None:
            return None
        try:
            return self.private_key_path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"Private key file not found at {self.private_key_path}"
            ) from exc

