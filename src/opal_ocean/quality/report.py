"""Generate quality reports for existing marts."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

from ..logging import get_logger, log_event
from ..paths import MARTS_DIR
from .checks import check_not_null, check_required_columns, check_value_range, summarize_results

LOGGER = get_logger("quality.report")


FACT_DAY_REQUIRED_COLUMNS = [
    "date",
    "weekday",
    "month",
    "commute_minutes",
    "opal_cost",
    "reliability",
    "pm25_mean",
    "rain_24h_mm",
    "beach_status",
    "beach_ok",
]


def run_quality_checks(run_id: str, run_dir: Path) -> Tuple[Path, Path]:
    fact_path = MARTS_DIR / "fact_day.parquet"
    if not fact_path.exists():
        raise FileNotFoundError("fact_day.parquet not found. Run `make marts` first.")

    frame = pd.read_parquet(fact_path)

    checks = [
        check_required_columns(frame, FACT_DAY_REQUIRED_COLUMNS),
        check_not_null(frame, ["date", "weekday", "commute_minutes", "reliability"]),
        check_value_range(frame, "reliability", 0.0, 1.0),
        check_value_range(frame, "pm25_mean", 0.0, None),
    ]

    summary_status = summarize_results(checks)

    manifest = {
        "run_id": run_id,
        "pipeline": "analyze",
        "generated_at": datetime.now(UTC).isoformat(),
        "inputs": [str(fact_path)],
        "status": summary_status,
    }

    quality = {
        "run_id": run_id,
        "pipeline": "analyze",
        "generated_at": datetime.now(UTC).isoformat(),
        "checks": checks,
        "status": summary_status,
    }

    manifest_path = run_dir / "run_manifest.json"
    quality_path = run_dir / "quality_report.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True))

    log_event(
        LOGGER,
        "quality_checks_completed",
        run_id=run_id,
        fact_day_rows=len(frame),
        status=summary_status,
        manifest=str(manifest_path),
        quality_report=str(quality_path),
    )

    return manifest_path, quality_path
