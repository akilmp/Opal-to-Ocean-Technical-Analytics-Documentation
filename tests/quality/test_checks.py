import pandas as pd

from opal_ocean.quality import checks


def test_required_columns():
    frame = pd.DataFrame({"date": ["2025-01-01"], "weekday": ["Wednesday"]})
    result = checks.check_required_columns(frame, ["date", "weekday", "commute_minutes"])
    assert result["status"] == "fail"
    assert result["missing"] == ["commute_minutes"]


def test_range_check_passes():
    frame = pd.DataFrame({"reliability": [0.5, 0.9]})
    result = checks.check_value_range(frame, "reliability", 0, 1)
    assert result["status"] == "pass"


def test_summarize_results():
    assert checks.summarize_results([
        {"status": "pass"},
        {"status": "pass"},
    ]) == "pass"
    assert checks.summarize_results([
        {"status": "pass"},
        {"status": "fail"},
    ]) == "fail"
