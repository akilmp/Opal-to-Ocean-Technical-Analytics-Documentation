"""Batch jobs for ingesting NSW datasets."""

from __future__ import annotations

import logging
import time
from typing import Iterable, Mapping, Optional

from .abs_sa2 import ABSSA2ContextClient
from .air_quality import AirQualityNSWClient
from .beachwatch import BeachwatchClient
from .pedestrian import CityOfSydneyPedestrianClient
from .tfnws import TfNSWGTFSRealtimeClient, TfNSWGTFSStaticClient
from .utils import configure_logging, get_env_list, log_job_summary, persist_dataframe

logger = logging.getLogger(__name__)

DEFAULT_STATIC_FEEDS = ["sydneytrains"]
DEFAULT_REALTIME_FEEDS = ["buses"]


def _ensure_logging() -> None:
    if not logging.getLogger().handlers:
        configure_logging()


def run_tfnws_static_job(
    feed_codes: Optional[Iterable[str]] = None,
) -> int:
    """Fetch TfNSW static GTFS feeds and persist to parquet."""

    _ensure_logging()
    feed_codes = list(feed_codes) if feed_codes else get_env_list(
        "TFNSW_STATIC_FEED_CODES", ",".join(DEFAULT_STATIC_FEEDS)
    )
    if not feed_codes:
        logger.warning("No TfNSW static feed codes configured; skipping job")
        return 0

    client = TfNSWGTFSStaticClient()
    total_rows = 0
    job_start = time.perf_counter()

    for feed_code in feed_codes:
        feed_start = time.perf_counter()
        df = client.fetch_feed(feed_code)
        destination = persist_dataframe(df, source="tfnws_static")
        duration = time.perf_counter() - feed_start
        row_count = len(df)
        total_rows += row_count
        log_job_summary(
            logger,
            source=f"tfnws_static:{feed_code}",
            row_count=row_count,
            duration_s=duration,
            destination=destination,
        )

    total_duration = time.perf_counter() - job_start
    log_job_summary(
        logger,
        source="tfnws_static_total",
        row_count=total_rows,
        duration_s=total_duration,
    )
    return total_rows


def run_tfnws_realtime_job(
    feed_codes: Optional[Iterable[str]] = None,
) -> int:
    """Fetch TfNSW realtime GTFS feeds and persist to parquet."""

    _ensure_logging()
    feed_codes = list(feed_codes) if feed_codes else get_env_list(
        "TFNSW_REALTIME_FEED_CODES", ",".join(DEFAULT_REALTIME_FEEDS)
    )
    if not feed_codes:
        logger.warning("No TfNSW realtime feed codes configured; skipping job")
        return 0

    client = TfNSWGTFSRealtimeClient()
    total_rows = 0
    job_start = time.perf_counter()

    for feed_code in feed_codes:
        feed_start = time.perf_counter()
        df = client.fetch_feed(feed_code)
        destination = persist_dataframe(df, source="tfnws_realtime")
        duration = time.perf_counter() - feed_start
        row_count = len(df)
        total_rows += row_count
        log_job_summary(
            logger,
            source=f"tfnws_realtime:{feed_code}",
            row_count=row_count,
            duration_s=duration,
            destination=destination,
        )

    total_duration = time.perf_counter() - job_start
    log_job_summary(
        logger,
        source="tfnws_realtime_total",
        row_count=total_rows,
        duration_s=total_duration,
    )
    return total_rows


def run_beachwatch_job(
    *,
    limit: int = 1000,
    filters: Optional[Mapping[str, object]] = None,
) -> int:
    """Fetch Beachwatch monitoring data and persist to parquet."""

    _ensure_logging()
    client = BeachwatchClient()
    start = time.perf_counter()
    df = client.fetch_measurements(limit=limit, filters=filters)
    destination = persist_dataframe(df, source="beachwatch")
    duration = time.perf_counter() - start
    row_count = len(df)
    log_job_summary(
        logger,
        source="beachwatch",
        row_count=row_count,
        duration_s=duration,
        destination=destination,
    )
    return row_count


def run_air_quality_job(
    *,
    limit: int = 1000,
    filters: Optional[Mapping[str, object]] = None,
) -> int:
    """Fetch Air Quality NSW data and persist to parquet."""

    _ensure_logging()
    client = AirQualityNSWClient()
    start = time.perf_counter()
    df = client.fetch_measurements(limit=limit, filters=filters)
    destination = persist_dataframe(df, source="air_quality_nsw")
    duration = time.perf_counter() - start
    row_count = len(df)
    log_job_summary(
        logger,
        source="air_quality_nsw",
        row_count=row_count,
        duration_s=duration,
        destination=destination,
    )
    return row_count


def run_pedestrian_counts_job(
    *,
    limit: int = 1000,
    filters: Optional[Mapping[str, object]] = None,
) -> int:
    """Fetch City of Sydney pedestrian counts and persist to parquet."""

    _ensure_logging()
    client = CityOfSydneyPedestrianClient()
    start = time.perf_counter()
    df = client.fetch_counts(limit=limit, filters=filters)
    destination = persist_dataframe(df, source="city_of_sydney_pedestrian")
    duration = time.perf_counter() - start
    row_count = len(df)
    log_job_summary(
        logger,
        source="city_of_sydney_pedestrian",
        row_count=row_count,
        duration_s=duration,
        destination=destination,
    )
    return row_count


def run_abs_sa2_context_job(
    *,
    limit: int = 1000,
    filters: Optional[Mapping[str, object]] = None,
) -> int:
    """Fetch ABS SA2 context data and persist to parquet."""

    _ensure_logging()
    client = ABSSA2ContextClient()
    start = time.perf_counter()
    df = client.fetch_context(limit=limit, filters=filters)
    destination = persist_dataframe(df, source="abs_sa2_context")
    duration = time.perf_counter() - start
    row_count = len(df)
    log_job_summary(
        logger,
        source="abs_sa2_context",
        row_count=row_count,
        duration_s=duration,
        destination=destination,
    )
    return row_count
