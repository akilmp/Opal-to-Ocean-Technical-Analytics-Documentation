"""Synthetic ingestion pipeline for Opal to Ocean."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from ..logging import get_logger, log_event
from ..paths import RAW_DIR

LOGGER = get_logger("ingest.pipeline")


COMMUTE_DATA = pd.DataFrame(
    [
        {
            "date": "2025-01-01",
            "total_trip_count": 4,
            "late_trip_count": 1,
            "commute_minutes": 92,
            "opal_cost": 15.8,
        },
        {
            "date": "2025-01-02",
            "total_trip_count": 4,
            "late_trip_count": 0,
            "commute_minutes": 88,
            "opal_cost": 15.1,
        },
    ]
)

ENV_DATA = pd.DataFrame(
    [
        {
            "date": "2025-01-01",
            "beach_status_raw": "Very Good",
            "pm25_mean": 8.2,
            "rain_24h_mm": 0.6,
        },
        {
            "date": "2025-01-02",
            "beach_status_raw": "Poor",
            "pm25_mean": 16.4,
            "rain_24h_mm": 12.1,
        },
    ]
)

PERSONAL_DATA = pd.DataFrame(
    [
        {
            "date": "2025-01-01",
            "steps": 10342,
            "sleep_hours": 7.5,
            "mood_1_5": 4,
            "caffeine_mg": 180,
        },
        {
            "date": "2025-01-02",
            "steps": 8543,
            "sleep_hours": 6.8,
            "mood_1_5": 3,
            "caffeine_mg": 220,
        },
    ]
)


def _write_dataset(frame: pd.DataFrame, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return len(frame)


def _build_manifest(run_id: str, outputs: List[Tuple[str, int]]) -> dict:
    return {
        "run_id": run_id,
        "pipeline": "ingest",
        "generated_at": datetime.now(UTC).isoformat(),
        "outputs": [
            {"dataset": name, "rows": rows, "path": str(RAW_DIR / f"{name}.csv")}
            for name, rows in outputs
        ],
    }


def _quality_checks(run_id: str, outputs: List[Tuple[str, int]]) -> dict:
    checks = []
    for name, rows in outputs:
        status = "pass" if rows > 0 else "fail"
        checks.append({"name": f"{name}_non_empty", "status": status, "rows": rows})
    return {
        "run_id": run_id,
        "pipeline": "ingest",
        "generated_at": datetime.now(UTC).isoformat(),
        "checks": checks,
    }


def run_ingest(run_id: str, run_dir: Path) -> tuple[Path, Path]:
    """Persist the synthetic raw datasets and emit manifest/quality reports."""
    datasets = {
        "commute": COMMUTE_DATA,
        "environment": ENV_DATA,
        "personal": PERSONAL_DATA,
    }

    saved_outputs: List[Tuple[str, int]] = []
    for name, frame in datasets.items():
        destination = RAW_DIR / f"{name}.csv"
        rows = _write_dataset(frame, destination)
        saved_outputs.append((name, rows))
        log_event(LOGGER, "dataset_written", dataset=name, path=str(destination), rows=rows)

    manifest = _build_manifest(run_id, saved_outputs)
    quality = _quality_checks(run_id, saved_outputs)

    manifest_path = run_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    quality_path = run_dir / "quality_report.json"
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True))

    return manifest_path, quality_path
