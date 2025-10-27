"""Streamlit dashboard for exploring the synthetic Sydney day analysis."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "marts"
FACT_DAY_PATH = DATA_DIR / "sydney_day_samples.csv"
SCENARIO_PATH = DATA_DIR / "what_if_scenarios.csv"


@st.cache_data(show_spinner=False)
def load_fact_day() -> pd.DataFrame:
    """Load the sample fact mart."""
    return pd.read_csv(FACT_DAY_PATH, parse_dates=["date"])


@st.cache_data(show_spinner=False)
def load_scenarios() -> pd.DataFrame:
    """Load pre-defined what-if scenarios."""
    return pd.read_csv(SCENARIO_PATH)


@st.cache_resource(show_spinner=False)
def train_model(df: pd.DataFrame) -> Pipeline:
    features = [
        "weekday",
        "weather_temp_c",
        "harbour_visits",
        "beach_time_hours",
        "commute_minutes",
        "cultural_events",
    ]
    target = "mood_score"
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["weekday"]),
            (
                "num",
                StandardScaler(),
                [
                    "weather_temp_c",
                    "harbour_visits",
                    "beach_time_hours",
                    "commute_minutes",
                    "cultural_events",
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
    target = "mood_score"
    features = df[[
        "weekday",
        "weather_temp_c",
        "harbour_visits",
        "beach_time_hours",
        "commute_minutes",
        "cultural_events",
    ]]

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
                "mood_score",
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
            "mood_score",
            "weather_temp_c",
            "beach_time_hours",
            "commute_minutes",
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
        "weather_temp_c",
        "harbour_visits",
        "beach_time_hours",
        "commute_minutes",
        "cultural_events",
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
    scenario_features["weather_temp_c"] += row["delta_weather_temp_c"]
    scenario_features["beach_time_hours"] += row["delta_beach_time_hours"]
    scenario_features["commute_minutes"] += row["delta_commute_minutes"]
    scenario_features["cultural_events"] += row["delta_cultural_events"]

    prediction = model.predict(scenario_features)[0]
    st.metric(
        "Predicted mood",
        f"{prediction:0.1f}",
        delta=f"{prediction - baseline_prediction:+0.1f} vs. baseline",
    )

    st.subheader("Custom adjusters")
    temp_adj = st.slider("Temperature change (°C)", -5.0, 5.0, float(row["delta_weather_temp_c"]))
    beach_adj = st.slider("Beach time change (hours)", -2.0, 2.0, float(row["delta_beach_time_hours"]))
    commute_adj = st.slider("Commute change (minutes)", -20.0, 20.0, float(row["delta_commute_minutes"]))
    culture_adj = st.slider("Cultural events change", -2.0, 2.0, float(row["delta_cultural_events"]))

    custom_features = baseline_df.copy()
    custom_features["weather_temp_c"] += temp_adj
    custom_features["beach_time_hours"] += beach_adj
    custom_features["commute_minutes"] += commute_adj
    custom_features["cultural_events"] += culture_adj
    custom_prediction = model.predict(custom_features)[0]
    st.metric(
        "Custom predicted mood",
        f"{custom_prediction:0.1f}",
        delta=f"{custom_prediction - baseline_prediction:+0.1f} vs. baseline",
    )


if __name__ == "__main__":
    st.set_page_config(page_title="Sydney Day Explorer", layout="wide")
    data = load_fact_day()
    scenario_data = load_scenarios()
    pipeline = train_model(data)

    page = st.sidebar.radio("Navigate", ("My Sydney Day", "What-ifs"))
    if page == "My Sydney Day":
        render_my_sydney_day(data, pipeline)
    else:
        render_what_ifs(data, pipeline, scenario_data)
