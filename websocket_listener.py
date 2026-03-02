"""Minimal WebSocket listener for Kalshi lifecycle notifications."""
from __future__ import annotations

import asyncio
import json
import logging
from urllib.parse import urlparse, urlunparse

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import KalshiSettings
from kalshi_client import AuthenticationConfigError, KalshiAPIClient
from logging_setup import configure_logging


def _build_ws_url(settings: KalshiSettings) -> str:
    """Convert the REST host to the websocket endpoint path."""
    parsed = urlparse(settings.host)
    return urlunparse(("wss", parsed.netloc, "/trade-api/ws/v2", "", "", ""))


async def _subscribe_market_lifecycle(websocket) -> None:
    """Subscribe to the market_lifecycle_v2 public channel."""
    payload = {
        "id": 1,
        "cmd": "subscribe",
        "params": {"channels": ["market_lifecycle_v2"]},
    }
    await websocket.send(json.dumps(payload))


async def listen_ws(settings: KalshiSettings | None = None, logger: logging.Logger | None = None) -> None:
    settings = settings or KalshiSettings()
    logger = logger or configure_logging(settings.log_level, log_dir=settings.log_directory)
    client = KalshiAPIClient(settings, logger=logger)
    if not client.auth_enabled:
        raise AuthenticationConfigError("WebSocket requires configured API credentials")

    ws_url = _build_ws_url(settings)
    headers = client.build_auth_headers("GET", ws_url)
    logger.info("Connecting to Kalshi websocket: %s", ws_url)

    async def _consume(ws):
        await _subscribe_market_lifecycle(ws)
        logger.info("WebSocket connected and subscribed to market_lifecycle_v2; awaiting messages...")
        async for message in ws:
            logger.info("WebSocket message: %s", message)

    try:
        try:
            async with connect(
                ws_url,
                extra_headers=list(headers.items()),
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as websocket:
                await _consume(websocket)
        except TypeError as exc:
            logger.warning(
                "websockets.connect rejected extra_headers (%s); retrying with additional_headers",
                exc,
            )
            async with connect(
                ws_url,
                additional_headers=list(headers.items()),
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            ) as websocket:
                await _consume(websocket)
    except (ConnectionClosedOK, ConnectionClosedError) as exc:
        logger.warning("WebSocket closed: %s", exc)
    except Exception as exc:  # pragma: no cover - safeguard for demo usage
        logger.error("WebSocket listener failed: %s", exc)
        raise


def main() -> None:
    try:
        asyncio.run(listen_ws())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
