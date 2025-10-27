import pandas as pd

from opal_ocean.transform.pipeline import transform_fact_day


def test_transform_fact_day_matches_golden():
    commute = pd.read_csv("tests/transformations/data/stg_commute_day.csv")
    environment = pd.read_csv("tests/transformations/data/stg_env_day.csv")
    personal = pd.read_csv("tests/transformations/data/stg_personal_day.csv")
    expected = pd.read_csv("tests/transformations/data/fact_day_expected.csv")

    actual = transform_fact_day(commute, environment, personal)

    pd.testing.assert_frame_equal(
        actual.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False
    )
