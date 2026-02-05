"""Reusable Kalshi API client wrapper with logging and error handling."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import kalshi_python
from kalshi_python.rest import ApiException

from config import KalshiSettings


class AuthenticationConfigError(RuntimeError):
    """Raised when an authenticated call is attempted without proper credentials."""


class KalshiAPIError(RuntimeError):
    """Raised when the Kalshi API responds with an error."""


@dataclass(frozen=True)
class CallMetadata:
    """Details captured for each API invocation."""

    operation: str
    authenticated: bool
    duration_ms: float


class KalshiAPIClient:
    """High-level wrapper that centralizes auth, logging, and error handling."""

    def __init__(self, settings: KalshiSettings, logger: Optional[logging.Logger] = None):
        self.settings = settings
        self.logger = logger or logging.getLogger("kalshi")
        self._configuration = kalshi_python.Configuration(host=settings.host)
        self._auth_enabled = False
        private_key = self.settings.read_private_key()
        if self.settings.api_key_id and private_key:
            self._configuration.api_key_id = self.settings.api_key_id
            self._configuration.private_key_pem = private_key
            self._auth_enabled = True
        self._client = kalshi_python.KalshiClient(self._configuration)

    def call(self, operation: str, *, authenticated: bool = False, **kwargs: Any) -> Any:
        """Dispatch an API call with consistent logging and exception mapping."""
        endpoint = self._resolve_operation(operation)
        if authenticated and not self._auth_enabled:
            raise AuthenticationConfigError(
                "Authenticated call requested but API credentials are missing"
            )
        return self._execute(endpoint, operation, authenticated, kwargs)

    def _resolve_operation(self, operation: str) -> Callable[..., Any]:
        try:
            return getattr(self._client, operation)
        except AttributeError as exc:
            raise AttributeError(f"Kalshi client has no operation '{operation}'") from exc

    def _execute(
        self,
        endpoint: Callable[..., Any],
        operation: str,
        authenticated: bool,
        params: dict[str, Any],
    ) -> Any:
        start = time.perf_counter()
        self.logger.debug(
            "Kalshi request started",
            extra={
                "operation": operation,
                "authenticated": authenticated,
                "params": params,
            },
        )
        try:
            response = endpoint(**params)
        except ApiException as api_exc:
            self._log_failure(operation, authenticated, start, api_exc)
            raise KalshiAPIError(
                f"Kalshi API error during '{operation}': {api_exc.reason}"
            ) from api_exc
        except Exception as exc:  # pragma: no cover - safeguard
            self._log_failure(operation, authenticated, start, exc)
            raise
        duration_ms = (time.perf_counter() - start) * 1000
        metadata = CallMetadata(operation, authenticated, duration_ms)
        self.logger.info(
            "Kalshi request completed",
            extra={
                "operation": metadata.operation,
                "authenticated": metadata.authenticated,
                "duration_ms": round(metadata.duration_ms, 2),
            },
        )
        return response

    def _log_failure(
        self,
        operation: str,
        authenticated: bool,
        start: float,
        error: Exception,
    ) -> None:
        duration_ms = (time.perf_counter() - start) * 1000
        self.logger.error(
            "Kalshi request failed",
            extra={
                "operation": operation,
                "authenticated": authenticated,
                "duration_ms": round(duration_ms, 2),
                "error": str(error),
            },
        )

    @property
    def auth_enabled(self) -> bool:
        return self._auth_enabled

