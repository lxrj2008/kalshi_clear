"""Minimal WebSocket listener for Kalshi lifecycle notifications."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from functools import partial
from urllib.parse import urlparse, urlunparse

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from config import KalshiSettings
from kalshi_client import AuthenticationConfigError, KalshiAPIClient
from logging_setup import configure_logging
from repositories.event_repository import EventRepository
from repositories.market_repository import MarketRepository
from services.events_service import EventsService
from services.markets_service import MarketsService


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

    markets_service = MarketsService(client, logger=logger)
    market_repo = MarketRepository(settings, logger=logger)
    events_service = EventsService(client, logger=logger)
    event_repo = EventRepository(settings, logger=logger)

    ws_url = _build_ws_url(settings)
    logger.info("Connecting to Kalshi websocket: %s", ws_url)

    async def _consume(ws):
        await _subscribe_market_lifecycle(ws)
        logger.info("WebSocket connected and subscribed to market_lifecycle_v2; awaiting messages...")
        async for message in ws:
            logger.info("WebSocket message: %s", message)
            await _handle_message(
                message,
                markets_service,
                market_repo,
                events_service,
                event_repo,
                logger,
            )

    backoff_seconds = 5
    attempt = 0
    while True:
        attempt += 1
        try:
            headers = client.build_auth_headers("GET", ws_url)
            async with connect(
                ws_url,
                additional_headers=list(headers.items()),
                ping_interval=10,
                ping_timeout=20,
                max_size=None,
            ) as websocket:
                logger.info("WebSocket connected (attempt %s); subscribed and consuming", attempt)
                await _consume(websocket)
                attempt = 0
                backoff_seconds = 5
        except (ConnectionClosedOK, ConnectionClosedError) as exc:
            logger.warning("WebSocket closed: %s; reconnecting in %ss", exc, backoff_seconds)
        except AuthenticationConfigError as exc:
            logger.error("WebSocket auth error: %s; cannot reconnect without valid credentials", exc)
            break
        except Exception as exc:  
            logger.error("WebSocket listener failed: %s; reconnecting in %ss", exc, backoff_seconds)

        try:
            await asyncio.sleep(backoff_seconds)
        except asyncio.CancelledError:
            break
        backoff_seconds = min(backoff_seconds * 2, 60)


async def _handle_message(
    raw_message: str,
    markets_service: MarketsService,
    market_repo: MarketRepository,
    events_service: EventsService,
    event_repo: EventRepository,
    logger: logging.Logger,
) -> None:
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("Skipping non-JSON websocket message")
        return

    if not isinstance(payload, dict):
        logger.debug("Ignoring unexpected payload type: %s", type(payload))
        return

    msg = payload.get("msg")
    if not isinstance(msg, dict):
        logger.debug("websocket payload missing msg body for type=%s", payload.get("type"))
        return

    payload_type = payload.get("type")
    if payload_type == "market_lifecycle_v2":
        await _handle_market_created(msg, markets_service, market_repo, logger)
    elif payload_type == "event_lifecycle":
        await _handle_event_created(msg, events_service, event_repo, logger)
    else:
        logger.debug("Ignoring websocket message type: %s", payload_type)


async def _handle_market_created(
    msg: dict,
    markets_service: MarketsService,
    market_repo: MarketRepository,
    logger: logging.Logger,
) -> None:
    event_type = msg.get("event_type")
    ticker = msg.get("market_ticker")
    if not ticker:
        logger.warning("market_lifecycle_v2 event missing market_ticker")
        return

    if event_type != "created":
        await _handle_market_update(msg, ticker, markets_service, market_repo, logger)
        return


    # created -> fetch full market and insert-if-absent

    loop = asyncio.get_running_loop()
    try:
        record = await asyncio.to_thread(markets_service.fetch_market_record, ticker)
    except Exception as exc:  
        logger.error("Failed to fetch market %s: %s", ticker, exc)
        return

    if record is None:
        logger.warning("No market record returned for ticker=%s", ticker)
        return

    try:
        await loop.run_in_executor(None, market_repo.save_markets, [record])
        logger.info("Saved new market record for ticker=%s", ticker)
    except Exception as exc:  
        logger.error("Failed to persist market %s: %s", ticker, exc)


async def _handle_market_update(
    msg: dict,
    ticker: str,
    markets_service: MarketsService,
    market_repo: MarketRepository,
    logger: logging.Logger,
) -> None:
    open_ts = msg.get("open_ts")
    close_ts = msg.get("close_ts")
    settlement_ts_raw = msg.get("settled_ts")
    determination_ts = msg.get("determination_ts")
    status = msg.get("event_type")
    result = msg.get("result")
    settlement_value = msg.get("settlement_value")

    def _ts_to_dt(value):
        if value is None:
            return None
        try:
            return datetime.fromtimestamp(int(value))
        except Exception:
            return None

    open_time = _ts_to_dt(open_ts)
    close_time = _ts_to_dt(close_ts)
    settlement_ts = _ts_to_dt(settlement_ts_raw)
    updated_time = _ts_to_dt(determination_ts)

    loop = asyncio.get_running_loop()
    try:
        update_task = partial(
            market_repo.update_market_fields,
            ticker,
            open_time=open_time,
            close_time=close_time,
            result=result,
            settlement_value=settlement_value,
            settlement_ts=settlement_ts,
            updated_time=updated_time,
            status=status,
        )
        updated = await loop.run_in_executor(None, update_task)
        logger.info("Updated market ticker=%s rows=%s", ticker, updated)
    except Exception as exc:  
        logger.error("Failed to update market %s: %s", ticker, exc)
        return

    if updated == 0:
        try:
            record = await asyncio.to_thread(markets_service.fetch_market_record, ticker)
        except Exception as exc:
            logger.error("Failed to refetch market %s after zero updates: %s", ticker, exc)
            return

        if record is None:
            logger.warning("Refetch returned no market record for ticker=%s", ticker)
            return

        try:
            await loop.run_in_executor(None, market_repo.save_markets, [record])
            logger.info("Inserted market ticker=%s after zero-update fallback", ticker)
        except Exception as exc:  
            logger.error("Failed to persist market %s after zero-update fallback: %s", ticker, exc)


async def _handle_event_created(
    msg: dict,
    events_service: EventsService,
    event_repo: EventRepository,
    logger: logging.Logger,
) -> None:
    event_ticker = msg.get("event_ticker")
    if not event_ticker:
        logger.warning("event_lifecycle event missing event_ticker; skipping")
        return

    loop = asyncio.get_running_loop()
    try:
        record = await asyncio.to_thread(events_service.fetch_event_record, event_ticker)
    except Exception as exc:
        logger.error("Failed to fetch event %s: %s", event_ticker, exc)
        return

    if record is None:
        logger.warning("No event record returned for event_ticker=%s", event_ticker)
        return

    try:
        await loop.run_in_executor(None, event_repo.save_events, [record])
        logger.info("Saved new event record for event_ticker=%s", event_ticker)
    except Exception as exc:
        logger.error("Failed to persist event %s: %s", event_ticker, exc)


def main() -> None:
    try:
        asyncio.run(listen_ws())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
