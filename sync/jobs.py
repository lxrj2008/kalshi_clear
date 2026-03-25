"""Scheduled sync jobs for KalshiClear.

This module keeps job definitions separate from the process/runtime wiring in main.py.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from time import sleep
from typing import Callable, Optional, TypeVar

from config import KalshiSettings
from kalshi_client import AuthenticationConfigError, KalshiAPIError
from models.category_record import CategoryRecord
from models.competition_record import CompetitionRecord
from models.competition_scope_record import CompetitionScopeRecord
from models.event_record import EventRecord
from models.market_record import MarketRecord
from models.scope_record import ScopeRecord
from models.sport_record import SportRecord
from models.sport_scope_record import SportScopeRecord
from models.tag_record import TagRecord
from repositories.base_repository import DatabaseSaveError
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


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 10
    max_backoff_seconds: int = 60

    def backoff_seconds(self, attempt: int) -> int:
        # attempt starts at 1
        return min(2**attempt, self.max_backoff_seconds)


TRecord = TypeVar("TRecord")


@dataclass(frozen=True)
class SyncPlan:
    """Describe a paginated sync job.

    The job provides three primitives:
    - fetch_page(cursor) -> (records, next_cursor)
    - flush(records) -> rows_persisted
    - reset_staging() -> None

    The runner handles buffering + retry/backoff + logging consistently.
    """

    name: str
    fetch_page: Callable[[Optional[str]], tuple[list[TRecord], Optional[str]]]
    flush: Callable[[list[TRecord]], int]
    reset_staging: Callable[[], None]
    buffer_target: int = 10_000
    retry_policy: RetryPolicy = RetryPolicy()


def _paginate_with_buffer_and_retry(
    *,
    plan: SyncPlan,
    logger: logging.Logger,
) -> int:
    """
    Generic paginator used by events/markets sync.

    This runner only understands cursors, buffering, retries, and flushing.
    All job-specific behavior is provided through the SyncPlan.
    """
    cursor: Optional[str] = None
    page = 1
    total_rows = 0
    buffer: list[TRecord] = []
    retry_attempt = 0

    plan.reset_staging()
    while True:
        try:
            records, cursor = plan.fetch_page(cursor)
            retry_attempt = 0
        except KalshiAPIError as api_error:
            if buffer:
                try:
                    upserted = plan.flush(buffer)
                    logger.info(
                        "Persisted %s %s rows before retry (buffer flush)",
                        upserted,
                        plan.name,
                    )
                    total_rows += upserted
                    buffer.clear()
                except DatabaseSaveError as db_error:
                    logger.error(
                        "Failed to persist buffered %s rows before retry (page=%s cursor=%s): %s",
                        plan.name,
                        page,
                        cursor,
                        db_error,
                    )

            retry_attempt += 1
            if retry_attempt > plan.retry_policy.max_retries:
                logger.error(
                    "%s request failed too many times on page %s (cursor=%s); aborting this run",
                    plan.name,
                    page,
                    cursor,
                )
                break
            backoff_seconds = plan.retry_policy.backoff_seconds(retry_attempt)
            logger.warning(
                "%s request failed on page %s (cursor=%s): %s; retrying after %ss",
                plan.name,
                page,
                cursor,
                api_error,
                backoff_seconds,
            )
            sleep(backoff_seconds)
            continue

        logger.info(
            "Fetched %s %s rows on page %s (next cursor=%s)",
            len(records),
            plan.name,
            page,
            cursor,
        )
        if records:
            buffer.extend(records)
            if len(buffer) >= plan.buffer_target:
                try:
                    upserted = plan.flush(buffer)
                    logger.info(
                        "Persisted %s %s rows to SQL Server (buffer flush)",
                        upserted,
                        plan.name,
                    )
                    total_rows += upserted
                    buffer.clear()
                except DatabaseSaveError as db_error:
                    logger.error(
                        "Failed to persist %s rows on page %s (cursor=%s): %s",
                        plan.name,
                        page,
                        cursor,
                        db_error,
                    )

        page += 1
        if not cursor:
            break

    if buffer:
        try:
            upserted = plan.flush(buffer)
            logger.info("Persisted %s remaining %s rows to SQL Server", upserted, plan.name)
            total_rows += upserted
        except DatabaseSaveError as db_error:
            logger.error("Failed to persist remaining %s rows: %s", plan.name, db_error)

    plan.reset_staging()
    logger.info("Completed %s sync; total rows persisted: %s", plan.name, total_rows)
    return total_rows


def run_filters_by_sport_persist(
    *,
    search_service: SearchService,
    sport_repository: SportRepository,
    competition_repository: CompetitionRepository,
    scope_repository: ScopeRepository,
    sport_scope_repository: SportScopeRepository,
    competition_scope_repository: CompetitionScopeRepository,
    logger: logging.Logger,
) -> None:
    """Persist filters-by-sport reference data into SQL Server."""
    filters_by_sport = search_service.fetch_filters_by_sport()

    sport_records: list[SportRecord] = []
    competition_records: list[CompetitionRecord] = []
    scope_records: list[ScopeRecord] = []
    sport_scope_records: list[SportScopeRecord] = []
    competition_scope_records: list[CompetitionScopeRecord] = []

    sport_seen: set[str] = set()
    competition_seen: set[str] = set()
    scope_seen: set[str] = set()
    sport_scope_seen: set[tuple[str, str]] = set()
    competition_scope_seen: set[tuple[str, str]] = set()

    for sport_name, sport_payload in filters_by_sport.items():
        if sport_name not in sport_seen:
            sport_seen.add(sport_name)
            sport_records.append(SportRecord(name=sport_name))

        sport_scopes = sport_payload.get("scopes") if isinstance(sport_payload, dict) else []
        if isinstance(sport_scopes, list):
            for scope in sport_scopes:
                scope_name = str(scope).strip()
                if not scope_name:
                    continue
                if scope_name not in scope_seen:
                    scope_seen.add(scope_name)
                    scope_records.append(ScopeRecord(name=scope_name))
                pair = (sport_name, scope_name)
                if pair not in sport_scope_seen:
                    sport_scope_seen.add(pair)
                    sport_scope_records.append(SportScopeRecord(sport_name=sport_name, scope_name=scope_name))

        competitions = sport_payload.get("competitions") if isinstance(sport_payload, dict) else {}
        if isinstance(competitions, dict):
            for competition_name, comp_payload in competitions.items():
                comp_name = str(competition_name).strip()
                if not comp_name:
                    continue
                if comp_name not in competition_seen:
                    competition_seen.add(comp_name)
                    competition_records.append(CompetitionRecord(name=comp_name, sport_name=sport_name))
                comp_scopes = comp_payload
                if isinstance(comp_scopes, list):
                    for scope in comp_scopes:
                        scope_name = str(scope).strip()
                        if not scope_name:
                            continue
                        if scope_name not in scope_seen:
                            scope_seen.add(scope_name)
                            scope_records.append(ScopeRecord(name=scope_name))
                        pair = (comp_name, scope_name)
                        if pair not in competition_scope_seen:
                            competition_scope_seen.add(pair)
                            competition_scope_records.append(
                                CompetitionScopeRecord(competition_name=comp_name, scope_name=scope_name)
                            )
                else:
                    logger.debug("Competition %s under sport %s has no scopes list", comp_name, sport_name)

    logger.info(
        "filters_by_sport parsed counts: sports=%s, competitions=%s, scopes=%s, sport_scopes=%s, competition_scopes=%s",
        len(sport_records),
        len(competition_records),
        len(scope_records),
        len(sport_scope_records),
        len(competition_scope_records),
    )

    if sport_records:
        sport_upserted = sport_repository.save_sports(sport_records)
        logger.info("Persisted %s sport rows to SQL Server", sport_upserted)
    if scope_records:
        scope_upserted = scope_repository.save_scopes(scope_records)
        logger.info("Persisted %s scope rows to SQL Server", scope_upserted)
    if competition_records:
        competition_upserted = competition_repository.save_competitions(competition_records)
        logger.info("Persisted %s competition rows to SQL Server", competition_upserted)
    if sport_scope_records:
        sport_scope_upserted = sport_scope_repository.save_sport_scopes(sport_scope_records)
        logger.info("Persisted %s sport-scope rows to SQL Server", sport_scope_upserted)
    if competition_scope_records:
        competition_scope_upserted = competition_scope_repository.save_competition_scopes(competition_scope_records)
        logger.info("Persisted %s competition-scope rows to SQL Server", competition_scope_upserted)


def run_tags_and_filters_full_job(
    *,
    search_service: SearchService,
    category_repository: CategoryRepository,
    tag_repository: TagRepository,
    sport_repository: SportRepository,
    competition_repository: CompetitionRepository,
    scope_repository: ScopeRepository,
    sport_scope_repository: SportScopeRepository,
    competition_scope_repository: CompetitionScopeRepository,
    logger: logging.Logger,
    settings: KalshiSettings,
) -> None:
    """Full tags + filters job (hourly)."""
    try:
        tags_by_categories = search_service.fetch_tags_by_categories()
        category_records = [CategoryRecord(name=category) for category in tags_by_categories.keys()]
        tag_records: list[TagRecord] = []
        for category_name, tag_list in tags_by_categories.items():
            if not tag_list:
                continue
            tag_records.extend(TagRecord(category=category_name, tag=tag) for tag in tag_list)
        if category_records:
            cat_upserted = category_repository.save_categories(category_records)
            logger.info("Persisted %s category rows to SQL Server", cat_upserted)
        if tag_records:
            tag_upserted = tag_repository.save_tags(tag_records)
            logger.info("Persisted %s tag rows to SQL Server", tag_upserted)
    except (KalshiAPIError, AuthenticationConfigError) as api_error:
        logger.error("Tags-by-categories request failed: %s", api_error)
    except DatabaseSaveError as db_error:
        logger.error("Failed to persist tag data: %s", db_error)

    try:
        run_filters_by_sport_persist(
            search_service=search_service,
            sport_repository=sport_repository,
            competition_repository=competition_repository,
            scope_repository=scope_repository,
            sport_scope_repository=sport_scope_repository,
            competition_scope_repository=competition_scope_repository,
            logger=logger,
        )
    except (KalshiAPIError, AuthenticationConfigError) as api_error:
        logger.error("Filters-by-sport request failed: %s", api_error)
    except DatabaseSaveError as db_error:
        logger.error("Failed to persist filters-by-sport data: %s", db_error)


def run_series_job(
    *,
    series_service: SeriesService,
    series_repository: SeriesRepository,
    logger: logging.Logger,
) -> None:
    try:
        records = series_service.list_series_records(include_volume=True)
        logger.info("Received %s series rows", len(records))
        inserted = series_repository.save_series(records)
        logger.info("Persisted %s series rows to SQL Server", inserted)
    except KalshiAPIError as api_error:
        logger.error("Series request failed: %s", api_error)
    except DatabaseSaveError as db_error:
        logger.error("Failed to persist series data: %s", db_error)


def run_events_job(
    *,
    events_service: EventsService,
    event_repository: EventRepository,
    logger: logging.Logger,
    status_filter: str = "open",
) -> int:
    logger.info("Applying events status filter: %s", status_filter)

    def fetch_page(cursor: Optional[str]) -> tuple[list[EventRecord], Optional[str]]:
        records, _milestones, next_cursor = events_service.list_event_records(
            limit=200,
            cursor=cursor,
            status=status_filter,
        )
        return records, next_cursor

    def flush(buffer: list[EventRecord]) -> int:
        return event_repository.save_events(buffer)

    plan = SyncPlan(
        name="event",
        fetch_page=fetch_page,
        flush=flush,
        reset_staging=event_repository.reset_staging,
    )
    return _paginate_with_buffer_and_retry(
        plan=plan,
        logger=logger,
    )


def run_markets_job(
    *,
    markets_service: MarketsService,
    market_repository: MarketRepository,
    logger: logging.Logger,
    status: str | None = None,
    use_created_filter: bool = False,
) -> int:
    min_created_ts = int(time.time()) - 18_000 if use_created_filter else None
    if status:
        logger.info("Applying markets status filter: %s", status)
    if min_created_ts is not None:
        logger.info("Applying markets min_created_ts filter: %s", min_created_ts)

    def fetch_page(cursor: Optional[str]) -> tuple[list[MarketRecord], Optional[str]]:
        filters: dict[str, object] = {"limit": 1000, "cursor": cursor}
        if status:
            filters["status"] = status
        if min_created_ts is not None:
            filters["min_created_ts"] = min_created_ts
        records, next_cursor = markets_service.list_market_records(**filters)
        return records, next_cursor

    def flush(buffer: list[MarketRecord]) -> int:
        return market_repository.save_markets(buffer)

    plan = SyncPlan(
        name="market",
        fetch_page=fetch_page,
        flush=flush,
        reset_staging=market_repository.reset_staging,
    )
    return _paginate_with_buffer_and_retry(
        plan=plan,
        logger=logger,
    )


def run_markets_cleanup_job(
    *,
    market_repository: MarketRepository,
    logger: logging.Logger,
    settled_days: int = 2,
) -> int:
    try:
        deleted = market_repository.delete_settled_before_days(days=settled_days)
        logger.info(
            "Deleted %s settled market rows older than %s day(s)",
            deleted,
            settled_days,
        )
        return deleted
    except DatabaseSaveError as db_error:
        logger.error("Failed to delete settled market rows: %s", db_error)
        return 0


__all__ = [
    "RetryPolicy",
    "run_tags_and_filters_full_job",
    "run_series_job",
    "run_events_job",
    "run_markets_job",
    "run_markets_cleanup_job",
]

