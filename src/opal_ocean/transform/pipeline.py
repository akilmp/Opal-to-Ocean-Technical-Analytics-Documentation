"""Transformation pipeline building marts from raw data."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Iterable, List, Tuple

import numpy as np
import pandas as pd

from ..ingest.parsers import UnknownStatusError, parse_beachwatch_status
from ..logging import get_logger, log_event
from ..paths import MARTS_DIR, RAW_DIR, STAGING_DIR
from ..quality.checks import (
    check_not_null,
    check_required_columns,
    check_value_range,
    summarize_results,
)

LOGGER = get_logger("transform.pipeline")
SYDNEY_TZ = ZoneInfo("Australia/Sydney")

FACT_DAY_COLUMNS: List[str] = [
    "date",
    "weekday",
    "month",
    "primary_mode",
    "commute_minutes",
    "opal_cost",
    "cost_per_trip",
    "total_trip_count",
    "late_trip_count",
    "reliability",
    "mean_delay_s",
    "max_delay_s",
    "pm25_mean",
    "pm10_mean",
    "rain_24h_mm",
    "temp_mean_c",
    "after_rain",
    "beach_status",
    "beach_ok",
    "enterococci_mean",
    "beach_site_id",
    "steps",
    "sleep_hours",
    "mood_1_5",
    "caffeine_mg",
    "meeting_count",
    "meeting_minutes",
]


def _load_parquet_dataset(path: Path, required_columns: Iterable[str] | None = None) -> pd.DataFrame:
    """Load a logical dataset from a directory of parquet partitions."""

    files = sorted(path.rglob("*.parquet"))
    if not files:
        columns = list(required_columns or [])
        return pd.DataFrame(columns=columns)

    frames = [pd.read_parquet(file) for file in files]
    frame = pd.concat(frames, ignore_index=True)
    if required_columns is not None:
        for column in required_columns:
            if column not in frame.columns:
                frame[column] = np.nan
        frame = frame[list(required_columns)]
    return frame


def _local_date(series: pd.Series) -> pd.Series:
    timestamps = pd.to_datetime(series, utc=True)
    return timestamps.dt.tz_convert(SYDNEY_TZ).dt.date


def _safe_status(value: object) -> str | float:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return np.nan
    try:
        return parse_beachwatch_status(str(value))
    except UnknownStatusError:
        LOGGER.warning("unknown_beachwatch_status", raw_value=value)
        return np.nan


def _boolean_check(frame: pd.DataFrame, column: str) -> dict:
    valid = frame[column].dropna().isin([True, False]).all()
    return {"name": f"boolean_{column}", "status": "pass" if valid else "fail", "column": column}


def build_stg_commute_day(trips: pd.DataFrame) -> pd.DataFrame:
    """Aggregate trip-level parquet feeds to the stg_commute_day grain."""

    if trips.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "primary_mode",
                "commute_minutes",
                "opal_cost",
                "late_trip_count",
                "total_trip_count",
                "mean_delay_s",
                "max_delay_s",
            ]
        )

    working = trips.copy()
    working["date"] = _local_date(working["trip_start_ts"])
    working["primary_mode"] = working["primary_mode"].fillna("unknown")
    working["trip_minutes"] = working["trip_minutes"].fillna(0.0)
    working["fare_amount"] = working["fare_amount"].fillna(0.0)
    working["delay_seconds"] = working["delay_seconds"].fillna(0.0)

    mode_rank = (
        working.groupby(["date", "primary_mode"], as_index=False)
        .agg(trip_count=("trip_id", "count"), avg_delay=("delay_seconds", "mean"))
        .assign(score=lambda df: df["trip_count"] - df["avg_delay"].fillna(0.0) / 100.0)
    )
    primary_mode = (
        mode_rank.sort_values(["date", "score"], ascending=[True, False])
        .drop_duplicates(subset=["date"], keep="first")
        .rename(columns={"primary_mode": "primary_mode"})
        [["date", "primary_mode"]]
    )

    aggregated = working.groupby("date", as_index=False).agg(
        commute_minutes=("trip_minutes", "sum"),
        opal_cost=("fare_amount", "sum"),
        late_trip_count=("delay_seconds", lambda s: (s > 60).sum()),
        total_trip_count=("trip_id", "count"),
        mean_delay_s=("delay_seconds", lambda s: s.replace(0, np.nan).mean()),
        max_delay_s=("delay_seconds", "max"),
    )

    result = aggregated.merge(primary_mode, on="date", how="left")
    result.sort_values("date", inplace=True)
    return result[[
        "date",
        "primary_mode",
        "commute_minutes",
        "opal_cost",
        "late_trip_count",
        "total_trip_count",
        "mean_delay_s",
        "max_delay_s",
    ]]


def build_stg_env_day(
    air_quality: pd.DataFrame, weather: pd.DataFrame, beachwatch: pd.DataFrame
) -> pd.DataFrame:
    """Aggregate environmental feeds to match stg_env_day."""

    if air_quality.empty:
        air_daily = pd.DataFrame(columns=["date", "pm25_mean", "pm10_mean", "primary_station_id"])
    else:
        air = air_quality.copy()
        air["date"] = _local_date(air["observation_ts"])
        station_day = (
            air.groupby(["date", "station_id"], as_index=False)
            .agg(pm25_mean=("pm25", "mean"), pm10_mean=("pm10", "mean"))
        )
        primary_station = (
            station_day.sort_values(["date", "pm25_mean"], ascending=[True, False])
            .drop_duplicates(subset=["date"], keep="first")
            .rename(columns={"station_id": "primary_station_id"})
            [["date", "primary_station_id"]]
        )
        air_daily = (
            station_day.groupby("date", as_index=False)
            .agg(pm25_mean=("pm25_mean", "mean"), pm10_mean=("pm10_mean", "mean"))
            .merge(primary_station, on="date", how="left")
        )

    if weather.empty:
        weather_daily = pd.DataFrame(columns=["date", "rain_24h_mm", "temp_mean_c"])
    else:
        w = weather.copy()
        w["date"] = _local_date(w["observation_ts"])
        weather_daily = (
            w.groupby("date", as_index=False)
            .agg(rain_24h_mm=("rain_mm", "sum"), temp_mean_c=("temperature_c", "mean"))
        )

    if beachwatch.empty:
        beach_daily = pd.DataFrame(
            columns=["date", "beach_status_primary", "enterococci_mean", "beach_site_id"]
        )
    else:
        b = beachwatch.copy()
        b["observation_ts"] = pd.to_datetime(b["observation_ts"], utc=True)
        b.sort_values("observation_ts", inplace=True)
        b["date"] = b["observation_ts"].dt.tz_convert(SYDNEY_TZ).dt.date
        beach_daily = (
            b.groupby("date", as_index=False)
            .agg(
                beach_status_primary=("status", "last"),
                enterococci_mean=("enterococci", "mean"),
                beach_site_id=("site_id", "last"),
            )
        )

    env = air_daily.merge(weather_daily, on="date", how="outer")
    env = env.merge(beach_daily, on="date", how="outer")
    env.sort_values("date", inplace=True)
    return env[[
        "date",
        "primary_station_id",
        "pm25_mean",
        "pm10_mean",
        "rain_24h_mm",
        "temp_mean_c",
        "beach_status_primary",
        "enterococci_mean",
        "beach_site_id",
    ]]


def build_stg_personal_day(personal: pd.DataFrame) -> pd.DataFrame:
    """Normalise personal tracking metrics to a daily grain."""

    columns_defaults = {
        "steps": 0,
        "sleep_hours": np.nan,
        "mood_1_5": np.nan,
        "caffeine_mg": 0,
        "meeting_count": 0,
        "meeting_minutes": 0,
    }
    if personal.empty:
        return pd.DataFrame(columns=["date", *columns_defaults.keys()])

    frame = personal.copy()
    for column, default in columns_defaults.items():
        if column not in frame.columns:
            frame[column] = default

    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    aggregated = (
        frame.groupby("date", as_index=False)
        .agg(
            steps=("steps", "sum"),
            sleep_hours=("sleep_hours", "mean"),
            mood_1_5=("mood_1_5", "mean"),
            caffeine_mg=("caffeine_mg", "sum"),
            meeting_count=("meeting_count", "sum"),
            meeting_minutes=("meeting_minutes", "sum"),
        )
    )
    aggregated["sleep_hours"] = aggregated["sleep_hours"].round(2)
    aggregated["mood_1_5"] = aggregated["mood_1_5"].round(2)
    aggregated.sort_values("date", inplace=True)
    return aggregated[[
        "date",
        "steps",
        "sleep_hours",
        "mood_1_5",
        "caffeine_mg",
        "meeting_count",
        "meeting_minutes",
    ]]


def transform_fact_day(
    commute_df: pd.DataFrame, environment_df: pd.DataFrame, personal_df: pd.DataFrame
) -> pd.DataFrame:
    """Compose the fact_day mart from staging dataframes."""

    if commute_df.empty and environment_df.empty and personal_df.empty:
        return pd.DataFrame(columns=FACT_DAY_COLUMNS)

    merged = commute_df.merge(environment_df, on="date", how="left")
    merged = merged.merge(personal_df, on="date", how="left")

    merged["date"] = pd.to_datetime(merged["date"])
    merged.sort_values("date", inplace=True)
    merged["weekday"] = merged["date"].dt.day_name()
    merged["month"] = merged["date"].dt.month.astype(int)

    for column in ["commute_minutes", "opal_cost", "mean_delay_s", "max_delay_s"]:
        merged[column] = merged[column].fillna(0.0)
    for column in ["total_trip_count", "late_trip_count"]:
        merged[column] = merged[column].fillna(0).astype(int)
    merged["primary_mode"] = merged["primary_mode"].fillna("unknown")

    merged["reliability"] = np.where(
        merged["total_trip_count"] > 0,
        1 - (merged["late_trip_count"] / merged["total_trip_count"].replace(0, np.nan)),
        1.0,
    )
    merged["reliability"] = merged["reliability"].clip(0, 1)
    merged["cost_per_trip"] = np.where(
        merged["total_trip_count"] > 0,
        merged["opal_cost"] / merged["total_trip_count"],
        0.0,
    ).round(2)

    merged["pm25_mean"] = merged["pm25_mean"].astype(float).fillna(0.0)
    merged["pm10_mean"] = merged["pm10_mean"].astype(float).fillna(0.0)
    merged["rain_24h_mm"] = merged["rain_24h_mm"].fillna(0.0)
    merged["temp_mean_c"] = merged["temp_mean_c"].astype(float).fillna(0.0)

    merged["after_rain"] = merged["rain_24h_mm"] > 0
    merged["beach_status"] = merged["beach_status_primary"].apply(_safe_status)
    merged["beach_ok"] = merged["beach_status"].isin(["Very good", "Good"])

    for column in ["steps", "caffeine_mg", "meeting_count", "meeting_minutes"]:
        merged[column] = merged[column].fillna(0).astype(int)
    merged["sleep_hours"] = merged["sleep_hours"].astype(float)
    merged["mood_1_5"] = merged["mood_1_5"].astype(float)
    merged["enterococci_mean"] = merged["enterococci_mean"].astype(float).fillna(0.0)

    fact_day = merged[FACT_DAY_COLUMNS].copy()
    fact_day["date"] = fact_day["date"].dt.strftime("%Y-%m-%d")
    fact_day.sort_values("date", inplace=True)
    fact_day.reset_index(drop=True, inplace=True)
    return fact_day


def _write_dataframe(frame: pd.DataFrame, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False)
    return target


def run_transforms(run_id: str, run_dir: Path) -> Tuple[Path, Path]:
    """Run the mart pipeline against the parquet raw zone."""

    raw_commute = _load_parquet_dataset(
        RAW_DIR / "commute_trips",
        [
            "trip_id",
            "trip_start_ts",
            "primary_mode",
            "trip_minutes",
            "fare_amount",
            "delay_seconds",
        ],
    )
    raw_air = _load_parquet_dataset(
        RAW_DIR / "air_quality",
        ["station_id", "observation_ts", "pm25", "pm10"],
    )
    raw_weather = _load_parquet_dataset(
        RAW_DIR / "weather",
        ["observation_ts", "rain_mm", "temperature_c"],
    )
    raw_beach = _load_parquet_dataset(
        RAW_DIR / "beachwatch",
        ["observation_ts", "site_id", "status", "enterococci"],
    )
    raw_personal = _load_parquet_dataset(
        RAW_DIR / "personal",
        [
            "date",
            "steps",
            "sleep_hours",
            "mood_1_5",
            "caffeine_mg",
            "meeting_count",
            "meeting_minutes",
        ],
    )

    stg_commute = build_stg_commute_day(raw_commute)
    stg_env = build_stg_env_day(raw_air, raw_weather, raw_beach)
    stg_personal = build_stg_personal_day(raw_personal)

    stage_outputs = {
        "stg_commute_day": stg_commute,
        "stg_env_day": stg_env,
        "stg_personal_day": stg_personal,
    }
    stage_paths = {
        name: _write_dataframe(df, STAGING_DIR / f"{name}.csv") for name, df in stage_outputs.items()
    }

    fact_day = transform_fact_day(stg_commute, stg_env, stg_personal)

    mart_path = _write_dataframe(fact_day, MARTS_DIR / "fact_day.csv")
    log_event(
        LOGGER,
        "fact_day_written",
        path=str(mart_path),
        rows=len(fact_day),
        run_id=run_id,
    )

    manifest_outputs = [
        {"dataset": name, "rows": int(df.shape[0]), "path": str(path)}
        for name, df in stage_outputs.items()
        for path in [stage_paths[name]]
    ]
    manifest_outputs.append({"dataset": "fact_day", "rows": len(fact_day), "path": str(mart_path)})

    quality_checks = [
        check_required_columns(fact_day, FACT_DAY_COLUMNS),
        check_not_null(
            fact_day,
            ["date", "weekday", "primary_mode", "commute_minutes", "opal_cost", "reliability"],
        ),
        check_value_range(fact_day, "reliability", 0.0, 1.0),
        check_value_range(fact_day, "commute_minutes", 0.0, None),
        check_value_range(fact_day, "opal_cost", 0.0, None),
        check_value_range(fact_day, "mean_delay_s", 0.0, None),
        check_value_range(fact_day, "pm25_mean", 0.0, None),
        check_value_range(fact_day, "pm10_mean", 0.0, None),
        check_value_range(fact_day, "steps", 0.0, None),
        _boolean_check(fact_day, "after_rain"),
    ]
    quality_status = summarize_results(quality_checks)

    manifest = {
        "run_id": run_id,
        "pipeline": "marts",
        "generated_at": datetime.now(UTC).isoformat(),
        "outputs": manifest_outputs,
        "status": quality_status,
    }
    quality = {
        "run_id": run_id,
        "pipeline": "marts",
        "generated_at": datetime.now(UTC).isoformat(),
        "checks": quality_checks,
        "status": quality_status,
    }

    manifest_path = run_dir / "run_manifest.json"
    quality_path = run_dir / "quality_report.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    quality_path.write_text(json.dumps(quality, indent=2, sort_keys=True))

    log_event(
        LOGGER,
        "run_transforms_completed",
        run_id=run_id,
        manifest=str(manifest_path),
        quality_report=str(quality_path),
        status=quality_status,
    )

    return manifest_path, quality_path
