"""Transformation pipeline building marts from raw data."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

from ..ingest.parsers import parse_beachwatch_status
from ..logging import get_logger, log_event
from ..paths import MARTS_DIR, RAW_DIR

LOGGER = get_logger("transform.pipeline")


def transform_fact_day(
    commute_df: pd.DataFrame, environment_df: pd.DataFrame, personal_df: pd.DataFrame
) -> pd.DataFrame:
    merged = commute_df.merge(environment_df, on="date", how="left")
    merged = merged.merge(personal_df, on="date", how="left")

    merged["date"] = pd.to_datetime(merged["date"])
    merged["weekday"] = merged["date"].dt.day_name()
    merged["month"] = merged["date"].dt.month.astype(int)

    merged["reliability"] = 1 - (merged["late_trip_count"] / merged["total_trip_count"])
    merged["beach_status"] = merged["beach_status_raw"].apply(parse_beachwatch_status)
    merged["beach_ok"] = merged["beach_status"].isin(["Very good", "Good"])

    fact_day = merged[
        [
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
            "steps",
            "sleep_hours",
            "mood_1_5",
            "caffeine_mg",
        ]
    ].copy()
    fact_day["date"] = fact_day["date"].dt.strftime("%Y-%m-%d")
    fact_day.sort_values("date", inplace=True)
    return fact_day


def run_transforms(run_id: str, run_dir: Path) -> Tuple[Path, Path]:
    commute = pd.read_csv(RAW_DIR / "commute.csv")
    environment = pd.read_csv(RAW_DIR / "environment.csv")
    personal = pd.read_csv(RAW_DIR / "personal.csv")

    fact_day = transform_fact_day(commute, environment, personal)

    mart_path = MARTS_DIR / "fact_day.parquet"
    mart_path.parent.mkdir(parents=True, exist_ok=True)
    fact_day.to_parquet(mart_path, index=False)

    log_event(LOGGER, "fact_day_written", path=str(mart_path), rows=len(fact_day))

    manifest = {
        "run_id": run_id,
        "pipeline": "marts",
        "generated_at": datetime.now(UTC).isoformat(),
        "outputs": [{"dataset": "fact_day", "rows": len(fact_day), "path": str(mart_path)}],
    }
    quality_checks = [
        {
            "name": "reliability_bounds",
            "status": "pass"
            if fact_day["reliability"].between(0, 1, inclusive="both").all()
            else "fail",
        },
        {
            "name": "positive_commute_minutes",
            "status": "pass" if (fact_day["commute_minutes"] > 0).all() else "fail",
        },
    ]
    quality = {
        "run_id": run_id,
        "pipeline": "marts",
        "generated_at": datetime.now(UTC).isoformat(),
        "checks": quality_checks,
    }

    manifest_path = run_dir / "run_manifest.json"
    quality_path = run_dir / "quality_report.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True))

    return manifest_path, quality_path
