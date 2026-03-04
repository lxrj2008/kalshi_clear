"""Standalone script (and importable helper) to exercise KalshiAPIClient.http_request."""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint
from typing import Any, Optional

from config import KalshiSettings
from kalshi_client import (
    AuthenticationConfigError,
    KalshiAPIClient,
    KalshiAPIError,
)
from logging_setup import configure_logging


# Reuse a single client across calls to benefit from connection pooling and auth setup.
_SHARED_CLIENT: KalshiAPIClient | None = None


@dataclass
class HttpRequestOptions:
    """Parameters governing a single HTTP request."""

    method: str = "GET"
    path: str = "/exchange/status"
    public: bool = False
    params: Optional[dict[str, Any]] = None
    json_body: Any = None
    data: Any = None
    headers: Optional[dict[str, str]] = None
    timeout: float = 30.0


def _parse_key_value_pairs(pairs: list[str]) -> dict[str, str]:
    """Convert CLI-supplied KEY:VALUE strings into a dictionary."""
    parsed: dict[str, str] = {}
    for pair in pairs:
        if ":" not in pair:
            raise ValueError(f"Header must use KEY:VALUE format: '{pair}'")
        key, value = pair.split(":", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _parse_assignment_pairs(pairs: list[str]) -> dict[str, str]:
    """Convert KEY=VALUE CLI flags into a dictionary."""
    parsed: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Parameter must use KEY=VALUE format: '{pair}'")
        key, value = pair.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _maybe_load_json(value: str | None) -> dict[str, object] | list[object] | None:
    if not value:
        return None
    return json.loads(value)


def _resolve_url(base_host: str, path: str) -> str:
    """Join the configured host with the provided path when needed."""
    if path.lower().startswith("http://") or path.lower().startswith("https://"):
        return path
    base = base_host.rstrip("/")
    relative = path.lstrip("/")
    return f"{base}/{relative}"


def execute_http_request(
    options: HttpRequestOptions,
    *,
    settings: KalshiSettings | None = None,
    logger=None,
    client: KalshiAPIClient | None = None,
) -> Any:
    """Run a single HTTP request using the shared Kalshi client wrapper."""
    provided_settings = settings or KalshiSettings()
    active_logger = logger or configure_logging(
        provided_settings.log_level,
        log_dir=provided_settings.log_directory,
    )
    global _SHARED_CLIENT
    active_client = client or _SHARED_CLIENT
    if active_client is None:
        active_client = KalshiAPIClient(provided_settings, logger=active_logger)
        _SHARED_CLIENT = active_client
    url = _resolve_url(provided_settings.host, options.path)
    response = active_client.http_request(
        options.method,
        url,
        authenticated=not options.public,
        params=options.params,
        json=options.json_body,
        data=options.data,
        headers=options.headers,
        timeout=options.timeout,
    )
    return response



def main() -> None:
    parser = argparse.ArgumentParser(description="Invoke Kalshi HTTP endpoints directly")
    parser.add_argument("--method", default="GET", help="HTTP method to use (default: GET)")
    parser.add_argument(
        "--path",
        default="/communications/quotes",
        help="Relative path or absolute URL for the request",
    )
    parser.add_argument(
        "--public",
        action="store_true",
        help="Call without Kalshi authentication headers",
    )
    parser.add_argument(
        "--params",
        help="Optional JSON object representing query parameters",
    )
    parser.add_argument(
        "--params-file",
        type=Path,
        help="Path to a JSON file containing query parameters",
    )
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        help="Single query parameter in KEY=VALUE form (repeatable)",
    )
    parser.add_argument(
        "--json",
        dest="json_body",
        help="Optional JSON payload for POST/PUT requests",
    )
    parser.add_argument(
        "--json-file",
        type=Path,
        help="Path to a JSON file used as the request body",
    )
    parser.add_argument(
        "--data",
        action="append",
        default=[],
        help="Form field in KEY=VALUE form (maps to requests' data=)",
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        help="Path to a file whose raw contents should be sent as the body",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Extra header in KEY:VALUE form (repeatable)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Request timeout in seconds (default: 30)",
    )
    args = parser.parse_args()

    settings = KalshiSettings()
    logger = configure_logging(settings.log_level, log_dir=settings.log_directory)
    query_params: dict[str, object] = {}
    if args.params:
        parsed = _maybe_load_json(args.params)
        if parsed:
            if not isinstance(parsed, dict):
                raise ValueError("--params JSON must be an object")
            query_params.update(parsed)
    if args.params_file:
        file_payload = json.loads(args.params_file.read_text(encoding="utf-8"))
        if not isinstance(file_payload, dict):
            raise ValueError("--params-file must contain a JSON object")
        query_params.update(file_payload)
    if args.param:
        query_params.update(_parse_assignment_pairs(args.param))
    query_payload = query_params or None

    json_payload = _maybe_load_json(args.json_body)
    if args.json_file:
        file_json = json.loads(args.json_file.read_text(encoding="utf-8"))
        if json_payload and isinstance(json_payload, dict) and isinstance(file_json, dict):
            json_payload = {**json_payload, **file_json}
        elif json_payload is None:
            json_payload = file_json
        else:
            raise ValueError("--json-file payload is incompatible with --json")

    form_fields = _parse_assignment_pairs(args.data) if args.data else None
    data_payload: object | None
    if args.data_file:
        data_payload = args.data_file.read_bytes()
    else:
        data_payload = form_fields

    headers = _parse_key_value_pairs(args.header) if args.header else {}

    options = HttpRequestOptions(
        method=args.method,
        path=args.path,
        public=args.public,
        params=query_payload,
        json_body=json_payload,
        data=data_payload,
        headers=headers or None,
        timeout=args.timeout,
    )

    try:
        response = execute_http_request(options, settings=settings, logger=logger)
    except (AuthenticationConfigError, KalshiAPIError, ValueError) as exc:
        logger.error("HTTP request test failed: %s", exc)
        raise SystemExit(1) from exc

    logger.info("HTTP request succeeded with status %s", response.status_code)
    print("\n=== Response Headers ===")
    for key, value in sorted(response.headers.items()):
        print(f"{key}: {value}")

    print("\n=== Response Body ===")
    content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type:
        try:
            pprint(response.json())
        except json.JSONDecodeError:
            print(response.text)
    else:
        print(response.text)


if __name__ == "__main__":
    main()
