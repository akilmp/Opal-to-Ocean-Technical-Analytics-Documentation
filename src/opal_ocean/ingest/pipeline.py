"""Live ingestion orchestration for the Opal to Ocean project."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

import pandas as pd

from ingest.air_quality import AirQualityNSWClient
from ingest.beachwatch import BeachwatchClient
from ingest.utils import persist_dataframe

from ..logging import get_logger, log_event
from ..paths import RAW_DIR
from .synthetic import (
    build_air_quality_measurements,
    build_beachwatch_observations,
    build_commute_trips,
    build_personal_metrics,
    build_weather_observations,
    generate_daily_profiles,
)

LOGGER = get_logger("ingest.pipeline")

INGEST_MODE_ENV = "OPAL_OCEAN_INGEST_MODE"
SYNTHETIC_DAYS_ENV = "OPAL_OCEAN_SYNTHETIC_DAYS"
SYNTHETIC_SEED_ENV = "OPAL_OCEAN_SYNTHETIC_SEED"

SYNTHETIC_DEFAULT_DAYS = 35
SYNTHETIC_DEFAULT_SEED = 7

DATASET_ORDER = ["commute_trips", "air_quality", "weather", "beachwatch", "personal"]
DATASET_SCHEMAS: Mapping[str, List[str]] = {
    "commute_trips": [
        "trip_id",
        "trip_start_ts",
        "primary_mode",
        "trip_minutes",
        "fare_amount",
        "delay_seconds",
    ],
    "air_quality": ["station_id", "observation_ts", "pm25", "pm10"],
    "weather": ["observation_ts", "rain_mm", "temperature_c"],
    "beachwatch": ["observation_ts", "site_id", "status", "enterococci"],
    "personal": [
        "date",
        "steps",
        "sleep_hours",
        "mood_1_5",
        "caffeine_mg",
        "meeting_count",
        "meeting_minutes",
    ],
}


@dataclass
class PartitionResult:
    """Metadata describing a single persisted partition."""

    path: Path
    rows: int
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class DatasetResult:
    """Aggregated ingestion details for a dataset."""

    name: str
    partitions: List[PartitionResult] = field(default_factory=list)
    metadata: Dict[str, object] = field(default_factory=dict)

    @property
    def rows(self) -> int:
        return sum(part.rows for part in self.partitions)

    def as_dict(self) -> Dict[str, object]:
        return {
            "dataset": self.name,
            "rows": self.rows,
            "metadata": self.metadata,
            "partitions": [
                {
                    "path": str(part.path),
                    "rows": part.rows,
                    "metadata": part.metadata,
                }
                for part in self.partitions
            ],
        }


def _read_int_env(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer; received {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(
            f"Environment variable {name} must be >= {minimum}; received {value}"
        )
    return value


def _resolve_ingest_mode() -> str:
    mode = os.getenv(INGEST_MODE_ENV, "synthetic").strip().lower()
    if mode not in {"synthetic", "live"}:
        raise ValueError(
            f"Unsupported ingest mode {mode!r}. Expected 'synthetic' or 'live'."
        )
    return mode


def _ensure_schema(dataset: str, frame: pd.DataFrame) -> pd.DataFrame:
    required_columns = DATASET_SCHEMAS.get(dataset, [])
    if not required_columns:
        return frame
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(
            f"Dataset {dataset} is missing required columns: {', '.join(missing)}"
        )
    return frame[required_columns].copy()


def _create_dataset_result(
    name: str,
    frame: pd.DataFrame,
    ingest_date: date,
    metadata: Mapping[str, object],
) -> DatasetResult:
    ordered = _ensure_schema(name, frame)
    dataset_metadata = dict(metadata)
    dataset_metadata["record_count"] = int(len(ordered))
    destination = persist_dataframe(
        ordered,
        source=name,
        ingest_date=ingest_date,
        base_dir=RAW_DIR,
    )
    partition = PartitionResult(path=destination, rows=len(ordered), metadata=dataset_metadata)
    return DatasetResult(name=name, partitions=[partition], metadata=dataset_metadata)


def _observation_date_metadata(series: pd.Series) -> Dict[str, object]:
    timestamps = pd.to_datetime(series, utc=True, errors="coerce").dropna()
    if timestamps.empty:
        return {}
    start = timestamps.min().date().isoformat()
    end = timestamps.max().date().isoformat()
    return {"start_date": start, "end_date": end}


def _generate_synthetic_data(ingest_date: date) -> tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, object]]]:
    days = _read_int_env(SYNTHETIC_DAYS_ENV, SYNTHETIC_DEFAULT_DAYS, minimum=1)
    seed = _read_int_env(SYNTHETIC_SEED_ENV, SYNTHETIC_DEFAULT_SEED, minimum=0)
    profiles = generate_daily_profiles(days=days, end_date=ingest_date, seed=seed)
    if profiles:
        start_date = profiles[0].date.isoformat()
        end_date = profiles[-1].date.isoformat()
    else:
        start_date = ingest_date.isoformat()
        end_date = ingest_date.isoformat()

    base_metadata: Dict[str, object] = {
        "mode": "synthetic",
        "seed": seed,
        "days": days,
        "start_date": start_date,
        "end_date": end_date,
    }

    frames: Dict[str, pd.DataFrame] = {}
    metadata: Dict[str, Dict[str, object]] = {}

    frames["commute_trips"] = build_commute_trips(profiles, seed=seed + 1)
    metadata["commute_trips"] = dict(base_metadata)

    frames["air_quality"] = build_air_quality_measurements(profiles, seed=seed + 2)
    metadata["air_quality"] = dict(base_metadata)

    frames["weather"] = build_weather_observations(profiles, seed=seed + 3)
    metadata["weather"] = dict(base_metadata)

    frames["beachwatch"] = build_beachwatch_observations(profiles, seed=seed + 4)
    metadata["beachwatch"] = dict(base_metadata)

    frames["personal"] = build_personal_metrics(profiles)
    metadata["personal"] = dict(base_metadata)

    return frames, metadata


def _normalize_dataframe(
    frame: pd.DataFrame,
    *,
    required: Iterable[str],
    synonyms: Mapping[str, Iterable[str]],
) -> pd.DataFrame:
    lower_map = {column.lower(): column for column in frame.columns}
    rename_map: Dict[str, str] = {}
    for target, candidates in synonyms.items():
        for candidate in candidates:
            key = candidate.lower()
            if key in lower_map:
                rename_map[lower_map[key]] = target
                break
    normalized = frame.rename(columns=rename_map)
    missing = [column for column in required if column not in normalized.columns]
    if missing:
        raise KeyError(f"Missing required columns: {', '.join(missing)}")
    return normalized[list(required)].copy()


def _fetch_live_air_quality(limit: int = 1000) -> pd.DataFrame:
    client = AirQualityNSWClient()
    raw = client.fetch_measurements(limit=limit)
    if raw.empty:
        return pd.DataFrame(columns=DATASET_SCHEMAS["air_quality"])
    normalized = _normalize_dataframe(
        raw,
        required=DATASET_SCHEMAS["air_quality"],
        synonyms={
            "station_id": ["station_id", "station", "stationcode", "station code"],
            "observation_ts": [
                "observation_ts",
                "datetime",
                "date_time",
                "timestamp",
                "observation_time",
                "datetimelocal",
            ],
            "pm25": ["pm25", "pm2_5", "pm_2_5", "pm2.5", "pm25_concentration"],
            "pm10": ["pm10", "pm_10", "pm10_concentration", "pm10value"],
        },
    )
    normalized["observation_ts"] = pd.to_datetime(
        normalized["observation_ts"], utc=True, errors="coerce"
    )
    normalized = normalized.dropna(subset=["observation_ts"])
    normalized["station_id"] = normalized["station_id"].astype(str)
    normalized["pm25"] = pd.to_numeric(normalized["pm25"], errors="coerce")
    normalized["pm10"] = pd.to_numeric(normalized["pm10"], errors="coerce")
    normalized = normalized.dropna(subset=["pm25", "pm10"])
    normalized.sort_values("observation_ts", inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized


def _fetch_live_beachwatch(limit: int = 1000) -> pd.DataFrame:
    client = BeachwatchClient()
    raw = client.fetch_measurements(limit=limit)
    if raw.empty:
        return pd.DataFrame(columns=DATASET_SCHEMAS["beachwatch"])
    normalized = _normalize_dataframe(
        raw,
        required=DATASET_SCHEMAS["beachwatch"],
        synonyms={
            "observation_ts": ["observation_ts", "datetime", "sample_date", "observation_time"],
            "site_id": ["site_id", "site", "beach", "location", "site_name"],
            "status": ["status", "advice", "state"],
            "enterococci": [
                "enterococci",
                "enterococci_cfu",
                "enterococci_cfu_100ml",
                "enterococci_count",
            ],
        },
    )
    normalized["observation_ts"] = pd.to_datetime(
        normalized["observation_ts"], utc=True, errors="coerce"
    )
    normalized = normalized.dropna(subset=["observation_ts"])
    normalized["site_id"] = normalized["site_id"].astype(str)
    normalized["status"] = normalized["status"].astype(str)
    normalized["status"] = normalized["status"].str.strip()
    normalized = normalized[normalized["status"].astype(str).str.len() > 0]
    normalized["enterococci"] = pd.to_numeric(normalized["enterococci"], errors="coerce")
    normalized.sort_values(["site_id", "observation_ts"], inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized


def _generate_live_overrides() -> tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, object]]]:
    frames: Dict[str, pd.DataFrame] = {}
    metadata: Dict[str, Dict[str, object]] = {}

    try:
        air_quality = _fetch_live_air_quality()
    except Exception as exc:  # pragma: no cover - defensive logging
        log_event(LOGGER, "live_air_quality_failed", error=str(exc))
    else:
        if not air_quality.empty:
            frames["air_quality"] = air_quality
            metadata["air_quality"] = {
                "mode": "live",
                "source": "air_quality_nsw",
                **_observation_date_metadata(air_quality["observation_ts"]),
            }
            log_event(LOGGER, "live_air_quality_ingested", rows=len(air_quality))
        else:
            log_event(LOGGER, "live_air_quality_empty")

    try:
        beachwatch = _fetch_live_beachwatch()
    except Exception as exc:  # pragma: no cover - defensive logging
        log_event(LOGGER, "live_beachwatch_failed", error=str(exc))
    else:
        if not beachwatch.empty:
            frames["beachwatch"] = beachwatch
            metadata["beachwatch"] = {
                "mode": "live",
                "source": "nsw_beachwatch",
                **_observation_date_metadata(beachwatch["observation_ts"]),
            }
            log_event(LOGGER, "live_beachwatch_ingested", rows=len(beachwatch))
        else:
            log_event(LOGGER, "live_beachwatch_empty")

    return frames, metadata


def _build_manifest(run_id: str, outputs: Iterable[DatasetResult]) -> Dict[str, object]:
    return {
        "run_id": run_id,
        "pipeline": "ingest",
        "generated_at": datetime.now(UTC).isoformat(),
        "outputs": [result.as_dict() for result in outputs],
    }


def _quality_checks(run_id: str, outputs: Iterable[DatasetResult]) -> Dict[str, object]:
    checks = []
    for result in outputs:
        check: Dict[str, object] = {
            "dataset": result.name,
            "rows": result.rows,
            "status": "pass" if result.rows > 0 else "fail",
            "partitions": [
                {
                    "path": str(part.path),
                    "rows": part.rows,
                    "metadata": part.metadata,
                }
                for part in result.partitions
            ],
        }
        if result.metadata:
            check["metadata"] = result.metadata
        checks.append(check)
    return {
        "run_id": run_id,
        "pipeline": "ingest",
        "generated_at": datetime.now(UTC).isoformat(),
        "checks": checks,
    }


def run_ingest(run_id: str, run_dir: Path) -> tuple[Path, Path]:
    """Execute the ingestion workflow and emit manifest/quality reports."""

    ingest_date = datetime.now(UTC).date()
    mode = _resolve_ingest_mode()

    synthetic_frames, synthetic_metadata = _generate_synthetic_data(ingest_date)
    frames = dict(synthetic_frames)
    metadata = {name: dict(meta) for name, meta in synthetic_metadata.items()}

    if mode == "live":
        live_frames, live_metadata = _generate_live_overrides()
        for name, frame in live_frames.items():
            frames[name] = frame
        for name, meta in live_metadata.items():
            metadata[name] = dict(meta)

    datasets: List[DatasetResult] = []
    for dataset_name in DATASET_ORDER:
        if dataset_name not in frames:
            continue
        dataset_result = _create_dataset_result(
            dataset_name,
            frames[dataset_name],
            ingest_date,
            metadata.get(dataset_name, {}),
        )
        datasets.append(dataset_result)
        log_event(
            LOGGER,
            "dataset_ingested",
            dataset=dataset_result.name,
            rows=dataset_result.rows,
            partitions=len(dataset_result.partitions),
            mode=metadata.get(dataset_name, {}).get("mode", "synthetic"),
        )

    manifest = _build_manifest(run_id, datasets)
    quality = _quality_checks(run_id, datasets)

    manifest_path = run_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    quality_path = run_dir / "quality_report.json"
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True))

    log_event(
        LOGGER,
        "ingest_run_completed",
        run_id=run_id,
        ingest_mode=mode,
        manifest=str(manifest_path),
        quality_report=str(quality_path),
    )

    return manifest_path, quality_path
