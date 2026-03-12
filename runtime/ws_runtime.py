"""WebSocket listener runtime helpers."""

from __future__ import annotations

import asyncio
from threading import Event, Thread

from config import KalshiSettings
from websocket_listener import listen_ws


def start_ws_listener_thread(*, settings: KalshiSettings, logger, stop_event: Event) -> Thread:
    """Start the websocket listener in a daemon thread."""

    def _run() -> None:
        try:
            asyncio.run(listen_ws(settings=settings, logger=logger, stop_event=stop_event))
        except Exception as exc:
            logger.error("WebSocket listener stopped: %s", exc)

    thread = Thread(target=_run, name="kalshi-ws-listener", daemon=True)
    thread.start()
    logger.info("WebSocket listener thread started")
    return thread


__all__ = ["start_ws_listener_thread"]

