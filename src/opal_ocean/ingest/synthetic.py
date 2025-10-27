"""Synthetic data generators for reproducible ingest runs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Iterable, List

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

SYDNEY_TZ = ZoneInfo("Australia/Sydney")


@dataclass(frozen=True)
class DayProfile:
    """High-level daily state used to coordinate synthetic datasets."""

    date: date
    primary_mode: str
    commute_minutes: float
    late_probability: float
    base_fare: float
    rain_mm: float
    temp_mean_c: float
    pm25_level: float
    pm10_level: float
    beach_risk: float
    enterococci_base: float
    steps: int
    sleep_hours: float
    mood: float
    caffeine_mg: int
    meeting_count: int
    meeting_minutes: int


def generate_daily_profiles(
    *,
    days: int,
    end_date: date,
    seed: int,
) -> List[DayProfile]:
    """Create correlated synthetic profiles spanning ``days`` ending at ``end_date``."""

    rng = np.random.default_rng(seed)
    start_date = end_date - timedelta(days=days - 1)
    modes = np.array(["train", "bus", "light_rail", "ferry"])
    mode_probs = np.array([0.5, 0.3, 0.15, 0.05])

    profiles: List[DayProfile] = []
    for offset in range(days):
        current_date = start_date + timedelta(days=offset)

        rain_flag = rng.random()
        rain_mm = float(np.round(rng.gamma(2.0, 2.2), 1)) if rain_flag < 0.35 else 0.0
        temp_mean_c = float(
            np.round(
                np.clip(rng.normal(23.5 - rain_mm * 0.12, 2.3), 12.0, 32.0),
                1,
            )
        )
        pm25_level = float(
            np.round(
                np.clip(rng.normal(9.0 + rain_mm * 0.4, 2.0), 4.0, 45.0),
                1,
            )
        )
        pm10_level = float(
            np.round(
                np.clip(pm25_level + rng.normal(6.0, 2.2), 5.0, 80.0),
                1,
            )
        )
        beach_risk = float(
            np.clip(0.18 + rain_mm / 22.0 + rng.normal(0.0, 0.06), 0.0, 1.0)
        )
        enterococci_base = float(
            np.round(np.clip(18.0 + beach_risk * 130 + rng.normal(0.0, 8.0), 5.0, 300.0), 1)
        )

        primary_mode = str(rng.choice(modes, p=mode_probs))
        commute_minutes = float(
            np.round(
                np.clip(
                    rng.normal(70.0 if primary_mode == "train" else 56.0, 7.5),
                    25.0,
                    120.0,
                ),
                1,
            )
        )
        late_probability = float(
            np.clip(0.12 + rain_mm / 42.0 + rng.normal(0.0, 0.035), 0.05, 0.65)
        )
        base_fare_lookup = {"train": 4.4, "bus": 3.4, "light_rail": 3.1, "ferry": 5.7}
        base_fare = float(
            np.round(
                np.clip(base_fare_lookup.get(primary_mode, 3.8) + rng.normal(0.0, 0.35), 1.5, 8.5),
                2,
            )
        )

        steps = int(max(3200, rng.normal(9800 - rain_mm * 85, 1400)))
        sleep_hours = float(
            np.round(np.clip(rng.normal(7.1 - rain_mm * 0.025, 0.55), 5.0, 9.5), 2)
        )
        mood = float(
            np.round(np.clip(rng.normal(3.7 - rain_mm * 0.045, 0.6), 1.0, 5.0), 2)
        )
        caffeine_mg = int(max(70, rng.normal(190 + rain_mm * 4.0, 40)))
        meeting_count = int(np.clip(round(rng.normal(4.0, 1.1)), 0, 8))
        meeting_minutes = int(
            max(0, meeting_count * float(np.clip(rng.normal(32.0, 9.0), 18.0, 75.0)))
        )

        profiles.append(
            DayProfile(
                date=current_date,
                primary_mode=primary_mode,
                commute_minutes=commute_minutes,
                late_probability=late_probability,
                base_fare=base_fare,
                rain_mm=rain_mm,
                temp_mean_c=temp_mean_c,
                pm25_level=pm25_level,
                pm10_level=pm10_level,
                beach_risk=beach_risk,
                enterococci_base=enterococci_base,
                steps=steps,
                sleep_hours=sleep_hours,
                mood=mood,
                caffeine_mg=caffeine_mg,
                meeting_count=meeting_count,
                meeting_minutes=meeting_minutes,
            )
        )

    return profiles


def build_commute_trips(profiles: Iterable[DayProfile], *, seed: int) -> pd.DataFrame:
    """Expand daily commute profiles into trip-level synthetic data."""

    rng = np.random.default_rng(seed)
    records: List[dict[str, object]] = []
    trip_counter = 1

    for profile in profiles:
        for half, hour in enumerate([7, 17]):
            local_time = datetime.combine(
                profile.date,
                time(hour, int(rng.integers(0, 45))),
                tzinfo=SYDNEY_TZ,
            )
            start_ts = local_time.astimezone(UTC)
            trip_minutes = float(
                np.round(
                    np.clip(rng.normal(profile.commute_minutes / 2, 5.5), 12.0, 120.0),
                    1,
                )
            )
            if rng.random() < profile.late_probability:
                delay_seconds = float(
                    np.round(np.clip(rng.normal(210.0, 80.0), 60.0, 1800.0), 0)
                )
            else:
                delay_seconds = float(
                    np.round(np.clip(rng.normal(35.0, 25.0), 0.0, 600.0), 0)
                )
            fare_amount = float(
                np.round(
                    np.clip(rng.normal(profile.base_fare, 0.55), 1.0, 9.5),
                    2,
                )
            )

            records.append(
                {
                    "trip_id": f"{profile.date.isoformat()}_{trip_counter:03d}",
                    "trip_start_ts": start_ts,
                    "primary_mode": profile.primary_mode,
                    "trip_minutes": trip_minutes,
                    "fare_amount": fare_amount,
                    "delay_seconds": delay_seconds,
                }
            )
            trip_counter += 1

        if rng.random() < 0.35:
            midday_mode = str(
                rng.choice([profile.primary_mode, "walk", "bus"], p=[0.5, 0.25, 0.25])
            )
            midday_time = datetime.combine(
                profile.date,
                time(12, int(rng.integers(0, 60))),
                tzinfo=SYDNEY_TZ,
            )
            records.append(
                {
                    "trip_id": f"{profile.date.isoformat()}_{trip_counter:03d}",
                    "trip_start_ts": midday_time.astimezone(UTC),
                    "primary_mode": midday_mode,
                    "trip_minutes": float(
                        np.round(np.clip(rng.normal(18.0, 4.5), 5.0, 60.0), 1)
                    ),
                    "fare_amount": float(
                        np.round(
                            np.clip(
                                0.0
                                if midday_mode == "walk"
                                else rng.normal(profile.base_fare * 0.55, 0.4),
                                0.0,
                                6.0,
                            ),
                            2,
                        )
                    ),
                    "delay_seconds": float(
                        np.round(np.clip(rng.normal(12.0, 10.0), 0.0, 180.0), 0)
                    ),
                }
            )
            trip_counter += 1

    return pd.DataFrame.from_records(records)


def build_air_quality_measurements(
    profiles: Iterable[DayProfile], *, seed: int
) -> pd.DataFrame:
    """Generate hourly-ish air quality observations for two stations."""

    rng = np.random.default_rng(seed)
    stations = ["ST001", "ST002"]
    hours = [2, 8, 14, 20]
    records: List[dict[str, object]] = []

    for profile in profiles:
        for station_index, station_id in enumerate(stations):
            station_pm25 = profile.pm25_level * (1.0 + station_index * 0.06)
            station_pm10 = profile.pm10_level * (1.0 + station_index * 0.08)
            for hour in hours:
                timestamp = datetime.combine(
                    profile.date, time(hour, 0), tzinfo=SYDNEY_TZ
                ).astimezone(UTC)
                records.append(
                    {
                        "station_id": station_id,
                        "observation_ts": timestamp,
                        "pm25": float(
                            np.round(
                                np.clip(rng.normal(station_pm25, 1.4), 1.0, 120.0),
                                2,
                            )
                        ),
                        "pm10": float(
                            np.round(
                                np.clip(rng.normal(station_pm10, 2.1), 2.0, 160.0),
                                2,
                            )
                        ),
                    }
                )

    return pd.DataFrame.from_records(records)


def build_weather_observations(
    profiles: Iterable[DayProfile], *, seed: int
) -> pd.DataFrame:
    """Generate sub-daily weather observations with distributed rainfall."""

    rng = np.random.default_rng(seed)
    hours = [0, 6, 12, 18]
    records: List[dict[str, object]] = []

    for profile in profiles:
        if profile.rain_mm > 0:
            rain_split = rng.dirichlet(np.ones(len(hours)))
        else:
            rain_split = np.zeros(len(hours))

        for idx, hour in enumerate(hours):
            timestamp = datetime.combine(
                profile.date, time(hour, 0), tzinfo=SYDNEY_TZ
            ).astimezone(UTC)
            rain_amount = float(
                np.round(profile.rain_mm * rain_split[idx], 2)
            )
            temperature = float(
                np.round(
                    np.clip(rng.normal(profile.temp_mean_c, 1.8), 8.0, 38.0),
                    1,
                )
            )
            records.append(
                {
                    "observation_ts": timestamp,
                    "rain_mm": rain_amount,
                    "temperature_c": temperature,
                }
            )

    return pd.DataFrame.from_records(records)


def _status_from_risk(risk: float) -> str:
    if risk >= 0.85:
        return "Closed"
    if risk >= 0.65:
        return "Poor"
    if risk >= 0.3:
        return "Good"
    return "Very good"


def build_beachwatch_observations(
    profiles: Iterable[DayProfile], *, seed: int
) -> pd.DataFrame:
    """Generate Beachwatch-style status observations for two popular sites."""

    rng = np.random.default_rng(seed)
    sites = ["Bondi", "Manly"]
    records: List[dict[str, object]] = []

    for profile in profiles:
        for site_index, site in enumerate(sites):
            site_risk = float(
                np.clip(profile.beach_risk + rng.normal(0.0, 0.05) + site_index * 0.04, 0.0, 1.0)
            )
            status = _status_from_risk(site_risk)
            # Vary casing to exercise parser robustness
            status_value = status if rng.random() > 0.35 else status.lower()
            enterococci = float(
                np.round(
                    np.clip(
                        profile.enterococci_base * (1.0 + site_index * 0.12)
                        + rng.normal(0.0, 12.0),
                        5.0,
                        400.0,
                    ),
                    1,
                )
            )
            for hour in (9, 15):
                timestamp = datetime.combine(
                    profile.date, time(hour + site_index, 0), tzinfo=SYDNEY_TZ
                ).astimezone(UTC)
                records.append(
                    {
                        "observation_ts": timestamp,
                        "site_id": site,
                        "status": status_value,
                        "enterococci": enterococci,
                    }
                )

    return pd.DataFrame.from_records(records)


def build_personal_metrics(profiles: Iterable[DayProfile]) -> pd.DataFrame:
    """Project profile wellness/activity values onto the personal daily dataset."""

    records = [
        {
            "date": profile.date.isoformat(),
            "steps": int(profile.steps),
            "sleep_hours": float(profile.sleep_hours),
            "mood_1_5": float(profile.mood),
            "caffeine_mg": int(profile.caffeine_mg),
            "meeting_count": int(profile.meeting_count),
            "meeting_minutes": int(profile.meeting_minutes),
        }
        for profile in profiles
    ]
    return pd.DataFrame.from_records(records)
