"""KalshiClear process entry point.

This file intentionally stays thin:
- build settings/logger/client
- wire services + repositories
- start websocket listener
- run initial sync + start scheduler
"""
from __future__ import annotations

from threading import Event
from time import sleep

from config import KalshiSettings
from kalshi_client import KalshiAPIClient
from logging_setup import configure_logging
from repositories.category_repository import CategoryRepository
from repositories.competition_repository import CompetitionRepository
from repositories.competition_scope_repository import CompetitionScopeRepository
from repositories.event_repository import EventRepository
from repositories.market_repository import MarketRepository
from repositories.scope_repository import ScopeRepository
from repositories.series_repository import SeriesRepository
from repositories.sport_repository import SportRepository
from repositories.sport_scope_repository import SportScopeRepository
from repositories.tag_repository import TagRepository
from services.events_service import EventsService
from services.markets_service import MarketsService
from services.series_service import SeriesService
from services.search_service import SearchService

from runtime.scheduler_runtime import add_default_jobs, build_scheduler
from runtime.ws_runtime import start_ws_listener_thread
from sync.jobs import (
    run_events_job,
    run_markets_job,
    run_markets_cleanup_job,
    run_series_job,
    run_tags_and_filters_full_job,
)


def main() -> None:
    settings = KalshiSettings()
    logger = configure_logging(settings.log_level, log_dir=settings.log_directory)
    client = KalshiAPIClient(settings, logger=logger)

    # Global stop signal for background components (scheduler, websocket listener).
    stop_event = Event()

    # Start websocket listener regardless of scheduler status (it will fail fast if auth missing).
    start_ws_listener_thread(settings=settings, logger=logger, stop_event=stop_event)

    # Build services
    series_service = SeriesService(client)
    events_service = EventsService(client, logger=logger)
    markets_service = MarketsService(client, logger=logger)
    search_service = SearchService(client, logger=logger)

    # Build repositories
    series_repository = SeriesRepository(settings, logger=logger)
    event_repository = EventRepository(settings, logger=logger)
    market_repository = MarketRepository(settings, logger=logger)

    secondary_db = settings.sqlserver_secondary_database
    category_repository = CategoryRepository(settings, logger=logger, database_name=secondary_db)
    tag_repository = TagRepository(settings, logger=logger, database_name=secondary_db)
    sport_repository = SportRepository(settings, logger=logger, database_name=secondary_db)
    competition_repository = CompetitionRepository(settings, logger=logger, database_name=secondary_db)
    scope_repository = ScopeRepository(settings, logger=logger, database_name=secondary_db)
    sport_scope_repository = SportScopeRepository(settings, logger=logger, database_name=secondary_db)
    competition_scope_repository = CompetitionScopeRepository(settings, logger=logger, database_name=secondary_db)

    scheduler = None
    if client.auth_enabled:
        # Wrap parameterized jobs into zero-arg callables for APScheduler.
        def tags_filters_job() -> None:
            run_tags_and_filters_full_job(
                search_service=search_service,
                category_repository=category_repository,
                tag_repository=tag_repository,
                sport_repository=sport_repository,
                competition_repository=competition_repository,
                scope_repository=scope_repository,
                sport_scope_repository=sport_scope_repository,
                competition_scope_repository=competition_scope_repository,
                logger=logger,
                settings=settings,
            )

        def series_job() -> None:
            run_series_job(series_service=series_service, series_repository=series_repository, logger=logger)

        def events_job() -> None:
            run_events_job(events_service=events_service, event_repository=event_repository, logger=logger)

        def markets_job(*, use_created_filter: bool = False) -> None:
            run_markets_job(
                markets_service=markets_service,
                market_repository=market_repository,
                logger=logger,
                status="open",
                use_created_filter=use_created_filter,
            )

        def markets_cleanup_job() -> None:
            run_markets_cleanup_job(
                market_repository=market_repository,
                logger=logger,
                settled_days=2,
            )

        # Initial run once on startup.
        markets_cleanup_job()
        tags_filters_job()
        series_job()
        events_job()
        markets_job(use_created_filter=False)

        scheduler = build_scheduler(logger=logger, settings=settings)
        add_default_jobs(
            scheduler=scheduler,
            run_tags_filters=tags_filters_job,
            run_series=series_job,
            run_events=events_job,
            run_markets=markets_job,
            run_markets_cleanup=markets_cleanup_job,
        )
        scheduler.start()
        logger.info(
            "Scheduler started: tags/filters hourly; series hourly; events hourly; markets hourly; markets cleanup hourly"
        )
    else:
        logger.warning("Skipping authenticated sync because credentials are missing.")

    if scheduler:
        try:
            while True:
                sleep(60)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Stop signal received; shutting down...")
            stop_event.set()
            scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
