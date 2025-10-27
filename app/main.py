"""Streamlit dashboard for exploring the synthetic Sydney day analysis."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

PROJECT_ROOT = Path(os.environ.get("OPAL_OCEAN_PROJECT_ROOT", Path(__file__).resolve().parent.parent))
MARTS_DIR = Path(os.environ.get("OPAL_OCEAN_MARTS_DIR", PROJECT_ROOT / "data" / "marts"))
FACT_DAY_PATH = MARTS_DIR / "fact_day.parquet"
SCENARIO_PATH = MARTS_DIR / "what_if_scenarios.csv"

logger = logging.getLogger(__name__)


@st.cache_data(show_spinner=False)
def load_fact_day() -> pd.DataFrame:
    """Load the sample fact mart."""
    if not FACT_DAY_PATH.exists():
        logger.error("fact_day mart missing at %s", FACT_DAY_PATH)
        raise FileNotFoundError(
            "fact_day.parquet not found. Run `make ingest && make marts` to build the mart."
        )

    df = pd.read_parquet(FACT_DAY_PATH)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "beach_ok" in df.columns:
        df["beach_ok"] = df["beach_ok"].astype(int)
    return df


@st.cache_data(show_spinner=False)
def load_scenarios() -> pd.DataFrame:
    """Load pre-defined what-if scenarios."""
    if not SCENARIO_PATH.exists():
        logger.warning("Scenario configuration missing at %s", SCENARIO_PATH)
        raise FileNotFoundError(
            "what_if_scenarios.csv not found. Re-run `make analyze` to materialise supporting files."
        )

    scenarios = pd.read_csv(SCENARIO_PATH)
    expected_columns = {
        "scenario",
        "description",
        "delta_commute_minutes",
        "delta_reliability",
        "delta_pm25_mean",
        "delta_rain_24h_mm",
        "delta_steps",
        "delta_sleep_hours",
        "delta_caffeine_mg",
    }
    missing = expected_columns.difference(scenarios.columns)
    if missing:
        logger.error("Scenario file missing expected columns: %s", ", ".join(sorted(missing)))
        raise ValueError(
            "Scenario file is missing required adjustment columns. Rebuild analytics artefacts."
        )
    return scenarios


@st.cache_resource(show_spinner=False)
def train_model(df: pd.DataFrame) -> Pipeline:
    features = [
        "weekday",
        "commute_minutes",
        "opal_cost",
        "reliability",
        "pm25_mean",
        "rain_24h_mm",
        "beach_ok",
        "steps",
        "sleep_hours",
        "caffeine_mg",
    ]
    target = "mood_1_5"
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["weekday"]),
            (
                "num",
                StandardScaler(),
                [
                    "commute_minutes",
                    "opal_cost",
                    "reliability",
                    "pm25_mean",
                    "rain_24h_mm",
                    "beach_ok",
                    "steps",
                    "sleep_hours",
                    "caffeine_mg",
                ],
            ),
        ]
    )
    model = Pipeline(steps=[("prep", preprocessor), ("regressor", LinearRegression())])
    model.fit(df[features], df[target])
    return model


def bootstrap_interval(
    pipeline: Pipeline,
    features: pd.DataFrame,
    target: pd.Series,
    samples: int = 200,
    alpha: float = 0.05,
    random_state: int = 123,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(random_state)
    preds = []
    for _ in range(samples):
        idx = rng.integers(0, len(features), len(features))
        X_s = features.iloc[idx]
        y_s = target.iloc[idx]
        pipeline.fit(X_s, y_s)
        preds.append(pipeline.predict(features))
    preds = np.vstack(preds)
    point = pipeline.fit(features, target).predict(features)
    lower = np.percentile(preds, 100 * alpha / 2, axis=0)
    upper = np.percentile(preds, 100 * (1 - alpha / 2), axis=0)
    return point, lower, upper


def render_my_sydney_day(df: pd.DataFrame, model: Pipeline) -> None:
    st.title("My Sydney Day")
    st.write(
        "Explore the synthetic daily mart and view point forecasts with confidence intervals."
    )
    target = "mood_1_5"
    feature_columns = [
        "weekday",
        "commute_minutes",
        "opal_cost",
        "reliability",
        "pm25_mean",
        "rain_24h_mm",
        "beach_ok",
        "steps",
        "sleep_hours",
        "caffeine_mg",
    ]
    features = df[feature_columns]

    point, lower, upper = bootstrap_interval(model, features, df[target])
    with st.expander("Prediction intervals", expanded=True):
        latest = df.copy()
        latest["prediction"] = point
        latest["lower_ci"] = lower
        latest["upper_ci"] = upper
        st.dataframe(
            latest[[
                "date",
                "weekday",
                "mood_1_5",
                "prediction",
                "lower_ci",
                "upper_ci",
            ]].sort_values("date", ascending=False),
            use_container_width=True,
        )

    st.subheader("Daily averages by weekday")
    weekday_summary = (
        df.assign(weekday=df["weekday"].astype("category"))
        .groupby("weekday")[[
            "mood_1_5",
            "commute_minutes",
            "opal_cost",
            "reliability",
            "pm25_mean",
            "rain_24h_mm",
            "steps",
            "sleep_hours",
            "caffeine_mg",
        ]]
        .mean()
        .round(2)
    )
    st.dataframe(weekday_summary, use_container_width=True)



def render_what_ifs(df: pd.DataFrame, model: Pipeline, scenarios: pd.DataFrame) -> None:
    st.title("What-ifs")
    st.write(
        "Use pre-defined scenarios or craft your own adjustments to see how mood predictions respond."
    )
    features = [
        "weekday",
        "commute_minutes",
        "opal_cost",
        "reliability",
        "pm25_mean",
        "rain_24h_mm",
        "beach_ok",
        "steps",
        "sleep_hours",
        "caffeine_mg",
    ]
    baseline = df[features].mean(numeric_only=True).to_dict()
    baseline["weekday"] = df["weekday"].mode().iat[0]
    baseline_df = pd.DataFrame([baseline])
    baseline_prediction = model.predict(baseline_df)[0]

    st.subheader("Scenario explorer")
    scenario_name = st.selectbox(
        "Choose a scenario", scenarios["scenario"].tolist(), index=0
    )
    row = scenarios.set_index("scenario").loc[scenario_name]
    st.caption(row["description"])

    scenario_features = baseline_df.copy()
    scenario_features["commute_minutes"] += row["delta_commute_minutes"]
    scenario_features["reliability"] = (
        scenario_features["reliability"] + row["delta_reliability"]
    ).clip(0.0, 1.0)
    scenario_features["pm25_mean"] += row["delta_pm25_mean"]
    scenario_features["rain_24h_mm"] += row["delta_rain_24h_mm"]
    scenario_features["steps"] += row["delta_steps"]
    scenario_features["sleep_hours"] += row["delta_sleep_hours"]
    scenario_features["caffeine_mg"] += row["delta_caffeine_mg"]

    prediction = model.predict(scenario_features)[0]
    st.metric(
        "Predicted mood",
        f"{prediction:0.1f}",
        delta=f"{prediction - baseline_prediction:+0.1f} vs. baseline",
    )

    st.subheader("Custom adjusters")
    commute_adj = st.slider(
        "Commute change (minutes)", -30.0, 30.0, float(row["delta_commute_minutes"])
    )
    reliability_adj = st.slider(
        "Reliability change", -0.3, 0.3, float(row["delta_reliability"]), step=0.01
    )
    pm25_adj = st.slider(
        "PM2.5 change (µg/m³)", -10.0, 10.0, float(row["delta_pm25_mean"])
    )
    rain_adj = st.slider(
        "Rain change (mm)", -30.0, 30.0, float(row["delta_rain_24h_mm"])
    )
    steps_adj = st.slider("Steps change", -5000, 5000, int(row["delta_steps"]))
    sleep_adj = st.slider(
        "Sleep change (hours)", -2.0, 2.0, float(row["delta_sleep_hours"]), step=0.1
    )
    caffeine_adj = st.slider(
        "Caffeine change (mg)", -300, 300, int(row["delta_caffeine_mg"])
    )

    custom_features = baseline_df.copy()
    custom_features["commute_minutes"] += commute_adj
    custom_features["reliability"] = (
        custom_features["reliability"] + reliability_adj
    ).clip(0.0, 1.0)
    custom_features["pm25_mean"] += pm25_adj
    custom_features["rain_24h_mm"] += rain_adj
    custom_features["steps"] += steps_adj
    custom_features["sleep_hours"] += sleep_adj
    custom_features["caffeine_mg"] += caffeine_adj
    custom_prediction = model.predict(custom_features)[0]
    st.metric(
        "Custom predicted mood",
        f"{custom_prediction:0.1f}",
        delta=f"{custom_prediction - baseline_prediction:+0.1f} vs. baseline",
    )


if __name__ == "__main__":
    st.set_page_config(page_title="Sydney Day Explorer", layout="wide")
    try:
        data = load_fact_day()
    except (FileNotFoundError, ValueError) as exc:
        st.error(str(exc))
        st.stop()

    try:
        scenario_data = load_scenarios()
    except (FileNotFoundError, ValueError) as exc:
        st.warning(str(exc))
        scenario_data = pd.DataFrame(
            [
                {
                    "scenario": "Baseline",
                    "description": "Using dataset averages; rebuild scenarios via `make analyze`.",
                    "delta_commute_minutes": 0.0,
                    "delta_reliability": 0.0,
                    "delta_pm25_mean": 0.0,
                    "delta_rain_24h_mm": 0.0,
                    "delta_steps": 0.0,
                    "delta_sleep_hours": 0.0,
                    "delta_caffeine_mg": 0.0,
                }
            ]
        )

    pipeline = train_model(data)

    page = st.sidebar.radio("Navigate", ("My Sydney Day", "What-ifs"))
    if page == "My Sydney Day":
        render_my_sydney_day(data, pipeline)
    else:
        render_what_ifs(data, pipeline, scenario_data)
