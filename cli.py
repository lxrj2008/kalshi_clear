"""Manual CLI to pull a single event or market by ticker and persist it."""
from __future__ import annotations

import sys
from pprint import pprint

from config import KalshiSettings
from kalshi_client import AuthenticationConfigError, KalshiAPIClient, KalshiAPIError
from logging_setup import configure_logging
from repositories.base_repository import DatabaseSaveError
from repositories.event_repository import EventRepository
from repositories.market_repository import MarketRepository
from repositories.series_repository import SeriesRepository
from services.events_service import EventsService
from services.markets_service import MarketsService
from services.series_service import SeriesService


def sync_single_event(ticker: str, events_service: EventsService, event_repository: EventRepository, logger) -> None:
    try:
        record = events_service.fetch_event_record(ticker)
        if not record:
            logger.warning("No event returned for ticker=%s", ticker)
            return
        pprint(record.to_dict())
        upserted = event_repository.save_events([record])
        logger.info("Persisted %s event row(s) for ticker=%s", upserted, ticker)
    except KalshiAPIError as api_error:
        logger.error("Event request failed for %s: %s", ticker, api_error)
    except DatabaseSaveError as db_error:
        logger.error("Failed to persist event %s: %s", ticker, db_error)


def sync_single_market(ticker: str, markets_service: MarketsService, market_repository: MarketRepository, logger) -> None:
    try:
        record = markets_service.fetch_market_record(ticker)
        if not record:
            logger.warning("No market returned for ticker=%s", ticker)
            return
        pprint(record.to_dict())
        upserted = market_repository.save_markets([record])
        logger.info("Persisted %s market row(s) for ticker=%s", upserted, ticker)
    except KalshiAPIError as api_error:
        logger.error("Market request failed for %s: %s", ticker, api_error)
    except DatabaseSaveError as db_error:
        logger.error("Failed to persist market %s: %s", ticker, db_error)


def sync_single_series(ticker: str, series_service: SeriesService, series_repository: SeriesRepository, logger) -> None:
    try:
        record = series_service.fetch_series_record(ticker)
        if not record:
            logger.warning("No series returned for ticker=%s", ticker)
            return
        pprint(record.to_dict())
        upserted = series_repository.save_series([record])
        logger.info("Persisted %s series row(s) for ticker=%s", upserted, ticker)
    except KalshiAPIError as api_error:
        logger.error("Series request failed for %s: %s", ticker, api_error)
    except DatabaseSaveError as db_error:
        logger.error("Failed to persist series %s: %s", ticker, db_error)


def choose_and_run() -> None:
    settings = KalshiSettings()
    logger = configure_logging(settings.log_level, log_dir=settings.log_directory)
    client = KalshiAPIClient(settings, logger=logger)

    if not client.auth_enabled:
        logger.error("Authentication is required for manual sync; please set credentials.")
        sys.exit(1)

    events_service = EventsService(client, logger=logger)
    markets_service = MarketsService(client, logger=logger)
    series_service = SeriesService(client, logger=logger)
    event_repository = EventRepository(settings, logger=logger)
    market_repository = MarketRepository(settings, logger=logger)
    series_repository = SeriesRepository(settings, logger=logger)

    menu = (
        "Select an operation:\n"
        "1) Fetch single event by ticker (/events/{event_ticker})\n"
        "2) Fetch single market by ticker (/markets/{ticker})\n"
        "3) Fetch single series by ticker (/series/{ticker})\n"
        "q) Quit\n"
        "Choice: "
    )

    while True:
        choice = input(menu).strip().lower()
        if choice == "1":
            ticker = input("Enter event ticker: ").strip()
            if not ticker:
                print("Ticker cannot be empty.")
                continue
            logger.info("Manual run: event %s", ticker)
            sync_single_event(ticker, events_service, event_repository, logger)
        elif choice == "2":
            ticker = input("Enter market ticker: ").strip()
            if not ticker:
                print("Ticker cannot be empty.")
                continue
            logger.info("Manual run: market %s", ticker)
            sync_single_market(ticker, markets_service, market_repository, logger)
        elif choice == "3":
            ticker = input("Enter series ticker: ").strip()
            if not ticker:
                print("Ticker cannot be empty.")
                continue
            logger.info("Manual run: series %s", ticker)
            sync_single_series(ticker, series_service, series_repository, logger)
        elif choice == "q":
            logger.info("Exiting manual CLI")
            break
        else:
            print("Invalid choice; please enter 1, 2, or q.")


if __name__ == "__main__":
    try:
        choose_and_run()
    except AuthenticationConfigError as auth_err:
        print(f"Authentication configuration error: {auth_err}")
        sys.exit(1)
