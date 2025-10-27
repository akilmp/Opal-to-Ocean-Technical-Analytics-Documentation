import json
import shutil
from pathlib import Path

import pandas as pd

from opal_ocean.paths import MARTS_DIR, RAW_DIR, STAGING_DIR
from opal_ocean.transform.pipeline import run_transforms


def _write_parquet(directory: Path, filename: str, frame: pd.DataFrame) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(directory / filename, index=False)


def test_run_transforms_builds_expected_fact_day(tmp_path) -> None:
    run_id = "test_run"
    run_dir = tmp_path

    for dataset in ["commute_trips", "air_quality", "weather", "beachwatch", "personal"]:
        base = RAW_DIR / dataset
        if base.exists():
            shutil.rmtree(base)
        base.mkdir(parents=True, exist_ok=True)

    commute_df = pd.DataFrame(
        [
            {
                "trip_id": "c1",
                "trip_start_ts": "2024-12-31T20:00:00+00:00",
                "primary_mode": "train",
                "trip_minutes": 40,
                "fare_amount": 5.5,
                "delay_seconds": 240,
            },
            {
                "trip_id": "c2",
                "trip_start_ts": "2024-12-31T21:00:00+00:00",
                "primary_mode": "train",
                "trip_minutes": 30,
                "fare_amount": 5.0,
                "delay_seconds": 120,
            },
            {
                "trip_id": "c3",
                "trip_start_ts": "2024-12-31T22:00:00+00:00",
                "primary_mode": "train",
                "trip_minutes": 22,
                "fare_amount": 5.3,
                "delay_seconds": 0,
            },
            {
                "trip_id": "c4",
                "trip_start_ts": "2024-12-31T22:15:00+00:00",
                "primary_mode": "bus",
                "trip_minutes": 0,
                "fare_amount": 0.0,
                "delay_seconds": 0,
            },
            {
                "trip_id": "c5",
                "trip_start_ts": "2025-01-01T13:00:00+00:00",
                "primary_mode": "bus",
                "trip_minutes": 44,
                "fare_amount": 7.4,
                "delay_seconds": 0,
            },
            {
                "trip_id": "c6",
                "trip_start_ts": "2025-01-01T13:30:00+00:00",
                "primary_mode": "bus",
                "trip_minutes": 44,
                "fare_amount": 7.7,
                "delay_seconds": 0,
            },
            {
                "trip_id": "c7",
                "trip_start_ts": "2025-01-01T14:00:00+00:00",
                "primary_mode": "bus",
                "trip_minutes": 0,
                "fare_amount": 0.0,
                "delay_seconds": 0,
            },
            {
                "trip_id": "c8",
                "trip_start_ts": "2025-01-01T14:30:00+00:00",
                "primary_mode": "bus",
                "trip_minutes": 0,
                "fare_amount": 0.0,
                "delay_seconds": 0,
            },
        ]
    )
    _write_parquet(RAW_DIR / "commute_trips", "trips.parquet", commute_df)

    air_df = pd.DataFrame(
        [
            {
                "station_id": "ST001",
                "observation_ts": "2024-12-31T14:00:00+00:00",
                "pm25": 8.2,
                "pm10": 14.5,
            },
            {
                "station_id": "ST002",
                "observation_ts": "2025-01-01T14:00:00+00:00",
                "pm25": 16.4,
                "pm10": 22.3,
            },
        ]
    )
    _write_parquet(RAW_DIR / "air_quality", "air.parquet", air_df)

    weather_df = pd.DataFrame(
        [
            {
                "observation_ts": "2024-12-31T14:00:00+00:00",
                "rain_mm": 0.2,
                "temperature_c": 23.1,
            },
            {
                "observation_ts": "2024-12-31T20:00:00+00:00",
                "rain_mm": 0.4,
                "temperature_c": 23.1,
            },
            {
                "observation_ts": "2025-01-01T13:00:00+00:00",
                "rain_mm": 6.1,
                "temperature_c": 21.4,
            },
            {
                "observation_ts": "2025-01-01T20:00:00+00:00",
                "rain_mm": 6.0,
                "temperature_c": 22.0,
            },
        ]
    )
    _write_parquet(RAW_DIR / "weather", "weather.parquet", weather_df)

    beach_df = pd.DataFrame(
        [
            {
                "observation_ts": "2024-12-31T14:00:00+00:00",
                "site_id": "Bondi",
                "status": "good",
                "enterococci": 10.0,
            },
            {
                "observation_ts": "2024-12-31T23:00:00+00:00",
                "site_id": "Bondi",
                "status": "very good",
                "enterococci": 14.0,
            },
            {
                "observation_ts": "2025-01-01T13:00:00+00:00",
                "site_id": "Manly",
                "status": "closed",
                "enterococci": 80.0,
            },
            {
                "observation_ts": "2025-01-01T23:00:00+00:00",
                "site_id": "Manly",
                "status": "poor",
                "enterococci": 96.0,
            },
        ]
    )
    _write_parquet(RAW_DIR / "beachwatch", "beach.parquet", beach_df)

    personal_df = pd.DataFrame(
        [
            {
                "date": "2025-01-01",
                "steps": 10342,
                "sleep_hours": 7.5,
                "mood_1_5": 4,
                "caffeine_mg": 180,
                "meeting_count": 3,
                "meeting_minutes": 150,
            },
            {
                "date": "2025-01-02",
                "steps": 8543,
                "sleep_hours": 6.8,
                "mood_1_5": 3,
                "caffeine_mg": 220,
                "meeting_count": 5,
                "meeting_minutes": 240,
            },
        ]
    )
    _write_parquet(RAW_DIR / "personal", "personal.parquet", personal_df)

    manifest_path, quality_path = run_transforms(run_id=run_id, run_dir=run_dir)

    fact_day_path = MARTS_DIR / "fact_day.csv"
    assert fact_day_path.exists()
    actual_fact = pd.read_csv(fact_day_path)
    expected_fact = pd.read_csv("tests/transformations/data/fact_day_expected.csv")
    pd.testing.assert_frame_equal(actual_fact, expected_fact, check_dtype=False)

    with manifest_path.open() as handle:
        manifest = json.load(handle)
    with quality_path.open() as handle:
        quality = json.load(handle)

    assert manifest["status"] == "pass"
    assert quality["status"] == "pass"
    dataset_names = {output["dataset"] for output in manifest["outputs"]}
    assert dataset_names == {"stg_commute_day", "stg_env_day", "stg_personal_day", "fact_day"}

    stage_files = [STAGING_DIR / "stg_commute_day.csv", STAGING_DIR / "stg_env_day.csv", STAGING_DIR / "stg_personal_day.csv"]
    for stage_file in stage_files:
        assert stage_file.exists()
