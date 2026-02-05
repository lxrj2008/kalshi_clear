"""Entry point demonstrating the reusable Kalshi API client framework."""
from __future__ import annotations

from pprint import pprint

from config import KalshiSettings
from kalshi_client import (
	AuthenticationConfigError,
	KalshiAPIClient,
	KalshiAPIError,
)
from logging_setup import configure_logging
from repositories.base_repository import DatabaseSaveError
from repositories.series_repository import SeriesRepository
from services.series_service import SeriesService


def main() -> None:
	settings = KalshiSettings()
	logger = configure_logging(settings.log_level, log_dir=settings.log_directory)
	client = KalshiAPIClient(settings, logger=logger)
	series_service = SeriesService(client)
	series_repository = SeriesRepository(settings, logger=logger)

	if client.auth_enabled:
		try:
			records = series_service.list_series_records(status="trading")
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
