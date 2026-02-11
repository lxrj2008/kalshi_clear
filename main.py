"""Entry point demonstrating the reusable Kalshi API client framework."""
from __future__ import annotations

import json
from pprint import pprint
from time import sleep

from config import KalshiSettings
from kalshi_client import (
	AuthenticationConfigError,
	KalshiAPIClient,
	KalshiAPIError,
)
from logging_setup import configure_logging
from repositories.base_repository import DatabaseSaveError
from repositories.event_repository import EventRepository
from repositories.market_repository import MarketRepository
from repositories.series_repository import SeriesRepository
from services.events_service import EventsService
from services.markets_service import MarketsService
from services.series_service import SeriesService


def main() -> None:
	settings = KalshiSettings()
	logger = configure_logging(settings.log_level, log_dir=settings.log_directory)
	client = KalshiAPIClient(settings, logger=logger)
	series_service = SeriesService(client)
	events_service = EventsService(client, logger=logger)
	markets_service = MarketsService(client, logger=logger)
	series_repository = SeriesRepository(settings, logger=logger)
	event_repository = EventRepository(settings, logger=logger)
	market_repository = MarketRepository(settings, logger=logger)

	if client.auth_enabled:
		try:
			records = series_service.list_series_records()
			logger.info("Received %s series rows", len(records))
			pprint([record.to_dict() for record in records[:5]])
			logger.debug(
				"Prepared SQL parameter sample",
				extra={"params": records[0].to_sql_params() if records else None},
			)
			inserted = series_repository.save_series(records)
			logger.info("Persisted %s series rows to SQL Server", inserted)
		except KalshiAPIError as api_error:
			logger.error("Series request failed: %s", api_error)
		except DatabaseSaveError as db_error:
			logger.error("Failed to persist series data: %s", db_error)
		try:
			cursor = None
			total_rows = 0
			page = 1
			while True:
				event_records, milestones, cursor = events_service.list_event_records(
					limit=200,
					cursor=cursor,
					with_nested_markets=False,
				)
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
					upserted = event_repository.save_events(event_records)
					logger.info("Persisted %s event rows to SQL Server", upserted)
					total_rows += upserted
				page += 1
				if not cursor:
					break
			logger.info("Completed event sync; total rows persisted: %s", total_rows)
		except KalshiAPIError as api_error:
			logger.error("Events request failed: %s", api_error)
		except DatabaseSaveError as db_error:
			logger.error("Failed to persist event data: %s", db_error)
		try:
			market_cursor = None
			market_total_rows = 0
			market_page = 1
			while True:
				market_records, market_cursor = markets_service.list_market_records(
					limit=1000,
					cursor=market_cursor,
				)
				logger.info(
					"Fetched %s markets on page %s (next cursor=%s)",
					len(market_records),
					market_page,
					market_cursor,
				)
				if market_page == 1:
					pprint([record.to_dict() for record in market_records[:5]])
				if market_records:
					upserted = market_repository.save_markets(market_records)
					logger.info("Persisted %s market rows to SQL Server", upserted)
					market_total_rows += upserted
				if not market_cursor:
					break
				market_page += 1
				sleep(0.01)
			logger.info(
				"Completed market sync; total rows persisted: %s",
				market_total_rows,
			)
		except KalshiAPIError as api_error:
			logger.error("Markets request failed: %s", api_error)
		except DatabaseSaveError as db_error:
			logger.error("Failed to persist market data: %s", db_error)
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


if __name__ == "__main__":
	main()
