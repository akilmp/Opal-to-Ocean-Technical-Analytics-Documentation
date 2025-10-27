"""Utility helpers for ingestion jobs."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

DEFAULT_LOGGING_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger for ingestion jobs."""

    logging.basicConfig(level=level, format=DEFAULT_LOGGING_FORMAT)


def ensure_directory(path: Path) -> None:
    """Create the directory path if it does not exist."""

    path.mkdir(parents=True, exist_ok=True)


def persist_dataframe(
    df: pd.DataFrame,
    source: str,
    *,
    ingest_date: Optional[date] = None,
    base_dir: Path = Path("data/raw"),
) -> Path:
    """Persist a dataframe as an append-only parquet partition."""

    ingest_date = ingest_date or date.today()
    partition_dir = base_dir / source / f"ingest_date={ingest_date.isoformat()}"
    ensure_directory(partition_dir)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    destination = partition_dir / f"part-{timestamp}.parquet"
    df.to_parquet(destination, index=False)
    return destination


def get_env_list(name: str, default: Optional[str] = None) -> List[str]:
    """Read a comma separated environment variable as a list."""

    raw = os.getenv(name, default or "")
    if not raw:
        return []
    return [entry.strip() for entry in raw.split(",") if entry.strip()]


def log_job_summary(
    logger: logging.Logger,
    *,
    source: str,
    row_count: int,
    duration_s: float,
    destination: Optional[Path] = None,
) -> None:
    """Emit a consistent summary for completed jobs."""

    message = (
        "Ingested %s rows from %s in %.2fs",
        row_count,
        source,
        duration_s,
    )
    if destination:
        logger.info("%s -> %s", message[0] % message[1:], destination)
    else:
        logger.info(message[0], *message[1:])
