"""Reusable data-quality checks for marts."""
from __future__ import annotations

from typing import Iterable, List

import pandas as pd


def check_required_columns(frame: pd.DataFrame, required: Iterable[str]) -> dict:
    missing = [col for col in required if col not in frame.columns]
    return {
        "name": "required_columns",
        "status": "pass" if not missing else "fail",
        "missing": missing,
    }


def check_not_null(frame: pd.DataFrame, columns: Iterable[str]) -> dict:
    null_columns = [col for col in columns if frame[col].isna().any()]
    return {
        "name": "not_null",
        "status": "pass" if not null_columns else "fail",
        "columns": null_columns,
    }


def check_value_range(
    frame: pd.DataFrame, column: str, minimum: float | None, maximum: float | None
) -> dict:
    series = frame[column]
    meets_min = True if minimum is None else (series >= minimum).all()
    meets_max = True if maximum is None else (series <= maximum).all()
    status = "pass" if meets_min and meets_max else "fail"
    return {
        "name": f"range_{column}",
        "status": status,
        "min": minimum,
        "max": maximum,
    }


def summarize_results(checks: List[dict]) -> str:
    return "pass" if all(check["status"] == "pass" for check in checks) else "fail"
