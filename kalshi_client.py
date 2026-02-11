"""Reusable Kalshi API client wrapper with logging and error handling."""
from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import kalshi_python
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from kalshi_python.rest import ApiException
from requests import RequestException

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
        parsed_host = urlparse(settings.host)
        self._base_path = (parsed_host.path or "").rstrip("/")
        self._private_key = None
        if self.settings.api_key_id and private_key:
            self._configuration.api_key_id = self.settings.api_key_id
            self._configuration.private_key_pem = private_key
            self._auth_enabled = True
            key_bytes = private_key.encode("utf-8") if isinstance(private_key, str) else private_key
            self._private_key = serialization.load_pem_private_key(
                key_bytes,
                password=None,
                backend=default_backend(),
            )
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
            f"Kalshi request failed：{str(error)}",
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

    @property
    def sdk_client(self) -> kalshi_python.KalshiClient:
        """Expose the raw Kalshi SDK client for specialized scenarios."""
        return self._client

    def http_request(
        self,
        method: str,
        url: str,
        *,
        authenticated: bool = True,
        params: Optional[dict[str, Any]] = None,
        json: Any = None,
        data: Any = None,
        headers: Optional[dict[str, str]] = None,
        timeout: float = 30.0,
    ) -> requests.Response:
        """Perform a raw HTTP request with Kalshi authentication headers."""
        prepared_url = self._prepare_url(url)
        method_upper = method.upper()
        request_headers: dict[str, str] = dict(headers or {})
        if authenticated:
            if not self._auth_enabled:
                raise AuthenticationConfigError(
                    "Authenticated call requested but API credentials are missing"
                )
            request_headers.update(self._build_auth_headers(method_upper, prepared_url))
        start = time.perf_counter()
        operation = f"{method_upper} {prepared_url}"
        self.logger.debug(
            "Kalshi HTTP request started",
            extra={
                "operation": operation,
                "authenticated": authenticated,
                "params": params,
            },
        )
        try:
            response = requests.request(
                method_upper,
                prepared_url,
                params=params,
                json=json,
                data=data,
                headers=request_headers,
                timeout=timeout,
            )
            response.raise_for_status()
        except RequestException as exc:
            self._log_failure(operation, authenticated, start, exc)
            raise KalshiAPIError(
                f"Kalshi HTTP error during '{operation}': {exc}"
            ) from exc
        duration_ms = (time.perf_counter() - start) * 1000
        self.logger.info(
            "Kalshi HTTP request completed",
            extra={
                "operation": operation,
                "authenticated": authenticated,
                "duration_ms": round(duration_ms, 2),
                "status_code": response.status_code,
            },
        )
        return response

    def _prepare_url(self, url: str) -> str:
        if url.lower().startswith(("http://", "https://")):
            return url
        base = self.settings.host.rstrip("/")
        path = url.lstrip("/")
        return f"{base}/{path}"

    def _build_auth_headers(self, method: str, url: str) -> dict[str, str]:
        if not self.settings.api_key_id or not self._private_key:
            raise AuthenticationConfigError("Kalshi credentials are not configured")
        timestamp_str = str(int(time.time() * 1000))
        parsed = urlparse(url)
        path_no_query = parsed.path.split("?")[0]
        message = f"{timestamp_str}{method}{path_no_query}"
        signature = self._private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        signature_b64 = base64.b64encode(signature).decode("utf-8")
        return {
            "KALSHI-ACCESS-KEY": self.settings.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature_b64,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_str,
        }

    

