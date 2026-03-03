"""Entry point demonstrating the reusable Kalshi API client framework."""
from __future__ import annotations

import asyncio
import json
from pprint import pprint
from threading import Thread
from time import sleep

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config import KalshiSettings
from kalshi_client import (
	AuthenticationConfigError,
	KalshiAPIClient,
	KalshiAPIError,
)
from http_request_demo import fetch_filters_by_sport, fetch_tags_by_categories
from logging_setup import configure_logging
from models.category_record import CategoryRecord
from models.competition_record import CompetitionRecord
from models.competition_scope_record import CompetitionScopeRecord
from models.scope_record import ScopeRecord
from models.sport_record import SportRecord
from models.sport_scope_record import SportScopeRecord
from models.tag_record import TagRecord
from repositories.base_repository import DatabaseSaveError
from repositories.competition_repository import CompetitionRepository
from repositories.competition_scope_repository import CompetitionScopeRepository
from repositories.event_repository import EventRepository
from repositories.market_repository import MarketRepository
from repositories.category_repository import CategoryRepository
from repositories.scope_repository import ScopeRepository
from repositories.series_repository import SeriesRepository
from repositories.sport_repository import SportRepository
from repositories.sport_scope_repository import SportScopeRepository
from repositories.tag_repository import TagRepository
from services.events_service import EventsService
from services.markets_service import MarketsService
from services.series_service import SeriesService
from websocket_listener import listen_ws


def main() -> None:
	settings = KalshiSettings()
	logger = configure_logging(settings.log_level, log_dir=settings.log_directory)
	client = KalshiAPIClient(settings, logger=logger)

	def _run_ws_listener() -> None:
		try:
			asyncio.run(listen_ws(settings=settings, logger=logger))
		except Exception as exc:  # pragma: no cover - background safety
			logger.error("WebSocket listener stopped: %s", exc)

	ws_thread = Thread(target=_run_ws_listener, name="kalshi-ws-listener", daemon=True)
	ws_thread.start()
	logger.info("WebSocket listener thread started")

	series_service = SeriesService(client)
	events_service = EventsService(client, logger=logger)
	markets_service = MarketsService(client, logger=logger)
	series_repository = SeriesRepository(settings, logger=logger)
	event_repository = EventRepository(settings, logger=logger)
	market_repository = MarketRepository(settings, logger=logger)
	category_repository = CategoryRepository(settings, logger=logger)
	tag_repository = TagRepository(settings, logger=logger)
	sport_repository = SportRepository(settings, logger=logger)
	competition_repository = CompetitionRepository(settings, logger=logger)
	scope_repository = ScopeRepository(settings, logger=logger)
	sport_scope_repository = SportScopeRepository(settings, logger=logger)
	competition_scope_repository = CompetitionScopeRepository(settings, logger=logger)
	# response1 = client.call(
    #         "get_settlements_without_preload_content", authenticated=True)
	# response=client.sdk_client.get_settlements()

	def run_tags_and_filters_job() -> None:
		try:
			tags_by_categories = fetch_tags_by_categories(settings=settings, logger=logger)
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
			filters_by_sport = fetch_filters_by_sport(settings=settings, logger=logger)
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
							sport_scope_records.append(
								SportScopeRecord(sport_name=sport_name, scope_name=scope_name)
							)

				competitions = sport_payload.get("competitions") if isinstance(sport_payload, dict) else {}
				if isinstance(competitions, dict):
					for competition_name, comp_payload in competitions.items():
						comp_name = str(competition_name).strip()
						if not comp_name:
							continue
						if comp_name not in competition_seen:
							competition_seen.add(comp_name)
							competition_records.append(
								CompetitionRecord(name=comp_name, sport_name=sport_name)
							)
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
									CompetitionScopeRecord(
										competition_name=comp_name,
										scope_name=scope_name,
									)
								)
						else:
							logger.debug(
								"Competition %s under sport %s has no scopes list", comp_name, sport_name
							)

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
				competition_scope_upserted = competition_scope_repository.save_competition_scopes(
					competition_scope_records
				)
				logger.info(
					"Persisted %s competition-scope rows to SQL Server",
					competition_scope_upserted,
				)
		except (KalshiAPIError, AuthenticationConfigError) as api_error:
			logger.error("Filters-by-sport request failed: %s", api_error)
		except DatabaseSaveError as db_error:
			logger.error("Failed to persist filters-by-sport data: %s", db_error)

	def run_series_job() -> None:
		try:
			records = series_service.list_series_records(include_volume=True)
			logger.info("Received %s series rows", len(records))
			pprint([record.to_dict() for record in records[:5]])
			inserted = series_repository.save_series(records)
			logger.info("Persisted %s series rows to SQL Server", inserted)
		except KalshiAPIError as api_error:
			logger.error("Series request failed: %s", api_error)
		except DatabaseSaveError as db_error:
			logger.error("Failed to persist series data: %s", db_error)

	def run_events_job() -> None:
		try:
			cursor = None
			total_rows = 0
			page = 1
			while True:
				try:
					event_records, milestones, cursor = events_service.list_event_records(
						limit=200,
						cursor=cursor,
					)
				except KalshiAPIError as api_error:
					logger.warning(
						"Events request failed on page %s (cursor=%s): %s; retrying next page",
						page,
						cursor,
						api_error,
					)
					sleep(1)
					continue
				logger.info(
					"Fetched %s events on page %s (next cursor=%s)",
					len(event_records),
					page,
					cursor,
				)
				if page == 1:
					pprint([record.to_dict() for record in event_records[:5]])
				if milestones:
					logger.info("Received %s milestones on page %s", len(milestones), page)
				if event_records:
					try:
						upserted = event_repository.save_events(event_records)
						logger.info("Persisted %s event rows to SQL Server", upserted)
						total_rows += upserted
					except DatabaseSaveError as db_error:
						logger.error(
							"Failed to persist event rows on page %s (cursor=%s): %s",
							page,
							cursor,
							db_error,
						)
						# continue to next page even if this batch failed
				page += 1
				if not cursor:
					break
			logger.info("Completed event sync; total rows persisted: %s", total_rows)
		except KalshiAPIError as api_error:
			logger.error("Events request failed: %s", api_error)
		except DatabaseSaveError as db_error:
			logger.error("Failed to persist event data: %s", db_error)

	def run_markets_job() -> None:
		try:
			market_cursor = None
			market_total_rows = 0
			market_page = 1
			while True:
				try:
					market_records, market_cursor = markets_service.list_market_records(
						limit=1000,
						cursor=market_cursor,
					)
				except KalshiAPIError as api_error:
					logger.warning(
						"Markets request failed on page %s (cursor=%s): %s; retrying next page",
						market_page,
						market_cursor,
						api_error,
					)
					sleep(1)
					continue
				logger.info(
					"Fetched %s markets on page %s (next cursor=%s)",
					len(market_records),
					market_page,
					market_cursor,
				)
				if market_page == 1:
					pprint([record.to_dict() for record in market_records[:5]])
				if market_records:
					try:
						upserted = market_repository.save_markets(market_records)
						logger.info("Persisted %s market rows to SQL Server", upserted)
						market_total_rows += upserted
					except DatabaseSaveError as db_error:
						logger.error(
							"Failed to persist market rows on page %s (cursor=%s): %s",
							market_page,
							market_cursor,
							db_error,
						)
						# continue to next page even if this batch failed
				if not market_cursor:
					break
				market_page += 1
			logger.info(
				"Completed market sync; total rows persisted: %s",
				market_total_rows,
			)
		except KalshiAPIError as api_error:
			logger.error("Markets request failed: %s", api_error)
		except DatabaseSaveError as db_error:
			logger.error("Failed to persist market data: %s", db_error)

	scheduler: BackgroundScheduler | None = None

	if client.auth_enabled:
		run_tags_and_filters_job()
		run_series_job()
		run_events_job()
		run_markets_job()

		scheduler = BackgroundScheduler(job_defaults={"max_instances": 1, "coalesce": True})
		scheduler.add_job(
			run_tags_and_filters_job,
			CronTrigger(hour=1, minute=0),
			id="tags_filters_job",
			replace_existing=True,
		)
		scheduler.add_job(
			run_series_job,
			CronTrigger(minute=0),
			id="series_job",
			replace_existing=True,
		)
		scheduler.add_job(
			run_events_job,
			CronTrigger(minute=0),
			id="events_job",
			replace_existing=True,
		)
		scheduler.add_job(
			run_markets_job,
			CronTrigger(hour=2, minute=0),
			id="markets_job",
			replace_existing=True,
		)
		scheduler.start()
		logger.info(
			"Scheduler started: tags/filters daily 01:00; series hourly; events hourly; markets daily 02:00"
		)
	else:
		logger.warning("Skipping authenticated example because credentials are missing.")

	try:
		heartbeat = client.call("get_exchange_status", authenticated=False)
		pprint(heartbeat)
	except AttributeError:
		logger.info(
			"Operation 'get_exchange_status' is unavailable in this client version."
		)
	except KalshiAPIError as api_error:
		logger.error("Public endpoint call failed: %s", api_error)
	except AuthenticationConfigError as auth_error:
		logger.error("Unexpected auth requirement: %s", auth_error)

	if scheduler:
		try:
			while True:
				sleep(60)
		except (KeyboardInterrupt, SystemExit):
			logger.info("Shutting down scheduler...")
			scheduler.shutdown(wait=False)


if __name__ == "__main__":
	main()
